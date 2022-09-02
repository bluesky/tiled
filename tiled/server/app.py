import asyncio
import collections
import logging
import os
import secrets
import sys
import urllib.parse
from functools import lru_cache, partial
from pathlib import Path

import anyio
from fastapi import APIRouter, FastAPI, HTTPException, Request, Response, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import FileResponse

from tiled.database.core import purge_expired

from ..authenticators import Mode
from ..media_type_registration import (
    compression_registry as default_compression_registry,
)
from ..utils import SHARE_TILED_PATH
from ..validation_registration import validation_registry as default_validation_registry
from .authentication import get_current_principal
from .compression import CompressionMiddleware
from .core import PatchedStreamingResponse
from .dependencies import (
    get_query_registry,
    get_root_tree,
    get_serialization_registry,
    get_validation_registry,
)
from .object_cache import NO_CACHE, ObjectCache
from .object_cache import logger as object_cache_logger
from .object_cache import set_object_cache
from .router import declare_search_router, router
from .settings import get_sessionmaker, get_settings
from .utils import (
    API_KEY_COOKIE_NAME,
    CSRF_COOKIE_NAME,
    get_authenticators,
    get_base_url,
    get_root_url,
    record_timing,
)

SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}
SENSITIVE_COOKIES = {
    API_KEY_COOKIE_NAME,
}
CSRF_HEADER_NAME = "x-csrf"
CSRF_QUERY_PARAMETER = "csrf"

logger = logging.getLogger(__name__)
logger.setLevel("INFO")
handler = logging.StreamHandler()
handler.setLevel("DEBUG")
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)


def custom_openapi(app):
    """
    The app's openapi method will be monkey-patched with this.

    This is the approach the documentation recommends.

    https://fastapi.tiangolo.com/advanced/extending-openapi/
    """
    from .. import __version__

    if app.openapi_schema:
        return app.openapi_schema
    # Customize heading.
    openapi_schema = get_openapi(
        title="Tiled",
        version=__version__,
        description="Structured data access service",
        routes=app.routes,
    )
    # Insert refreshUrl.
    openapi_schema["components"]["securitySchemes"]["OAuth2PasswordBearer"]["flows"][
        "password"
    ]["refreshUrl"] = "token/refresh"
    app.openapi_schema = openapi_schema
    return app.openapi_schema


def build_app(
    tree,
    authentication=None,
    server_settings=None,
    query_registry=None,
    serialization_registry=None,
    compression_registry=None,
    validation_registry=None,
):
    """
    Serve a Tree

    Parameters
    ----------
    tree : Tree
    authentication: dict, optional
        Dict of authentication configuration.
    authenticators: list, optional
        List of authenticator classes (one per support identity provider)
    server_settings: dict, optional
        Dict of other server configuration.
    """
    authentication = authentication or {}
    authenticators = {
        spec["provider"]: spec["authenticator"]
        for spec in authentication.get("providers", [])
    }
    server_settings = server_settings or {}
    query_registry = query_registry or get_query_registry()
    compression_registry = compression_registry or default_compression_registry
    validation_registry = validation_registry or default_validation_registry

    app = FastAPI()

    if SHARE_TILED_PATH:
        # If the distribution includes static assets, serve UI routes.

        @app.get("/ui/{path:path}")
        async def ui(path):
            response = await lookup_file(path)
            return response

        async def lookup_file(path, try_app=True):
            if not path:
                path = "index.html"
            full_path = Path(SHARE_TILED_PATH, "ui", path)
            try:
                stat_result = await anyio.to_thread.run_sync(os.stat, full_path)
            except PermissionError:
                raise HTTPException(status_code=401)
            except FileNotFoundError:
                # This may be a URL that has meaning to the client-side application,
                # such as /ui/node/metadata/a/b/c.
                # Serve index.html and let the client-side application sort it out.
                if try_app:
                    response = await lookup_file("index.html", try_app=False)
                    return response
                raise HTTPException(status_code=404)
            except OSError:
                raise
            return FileResponse(
                full_path,
                stat_result=stat_result,
                method="GET",
                status_code=200,
            )

        app.mount(
            "/static",
            StaticFiles(directory=Path(SHARE_TILED_PATH, "static")),
            name="ui",
        )
        templates = Jinja2Templates(Path(SHARE_TILED_PATH, "templates"))

        @app.get("/", response_class=HTMLResponse)
        async def index(
            request: Request,
            # This dependency is here because it runs the code that moves
            # API key from the query parameter to a cookie (if it is valid).
            principal=Security(get_current_principal, scopes=[]),
        ):
            if request.headers.get("user-agent", "").startswith("python-tiled"):
                # This results in an error message like
                # ClientError: 400: To connect from a Python client, use
                # http://localhost:8000/api not http://localhost:8000/?root_path=true
                raise HTTPException(
                    status_code=400,
                    detail=f"To connect from a Python client, use {get_base_url(request)} not",
                )
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    # This is used to fill in the Python code sample with the API URL.
                    "api_url": get_base_url(request),
                    # This is used to construct the link to the React UI.
                    "root_url": get_root_url(request),
                    # If defined, this adds a Binder link to the page.
                    "binder_link": os.getenv("TILED_BINDER_LINK"),
                },
            )

    app.state.allow_origins = []
    app.include_router(router, prefix="/api")

    # The Tree and Authenticator have the opportunity to add custom routes to
    # the server here. (Just for example, a Tree of BlueskyRuns uses this
    # hook to add a /documents route.) This has to be done before dependency_overrides
    # are processed, so we cannot just inject this configuration via Depends.
    for custom_router in getattr(tree, "include_routers", []):
        app.include_router(custom_router, prefix="/api")

    if authentication.get("providers", []):
        # Delay this imports to avoid delaying startup with the SQL and cryptography
        # imports if they are not needed.
        from .authentication import (
            base_authentication_router,
            build_auth_code_route,
            build_handle_credentials_route,
            oauth2_scheme,
        )

        # For the OpenAPI schema, inject a OAuth2PasswordBearer URL.
        first_provider = authentication["providers"][0]["provider"]
        oauth2_scheme.model.flows.password.tokenUrl = (
            f"/api/auth/provider/{first_provider}/token"
        )
        # Authenticators provide Router(s) for their particular flow.
        # Collect them in the authentication_router.
        authentication_router = APIRouter()
        # This adds the universal routes like /session/refresh and /session/revoke.
        # Below we will add routes specific to our authentication providers.
        authentication_router.include_router(base_authentication_router)
        for spec in authentication["providers"]:
            provider = spec["provider"]
            authenticator = spec["authenticator"]
            mode = authenticator.mode
            if mode == Mode.password:
                authentication_router.post(f"/provider/{provider}/token")(
                    build_handle_credentials_route(authenticator, provider)
                )
            elif mode == Mode.external:
                authentication_router.get(f"/provider/{provider}/code")(
                    build_auth_code_route(authenticator, provider)
                )
                authentication_router.post(f"/provider/{provider}/code")(
                    build_auth_code_route(authenticator, provider)
                )
            else:
                raise ValueError(f"unknown authentication mode {mode}")
            for custom_router in getattr(authenticator, "include_routers", []):
                authentication_router.include_router(
                    custom_router, prefix=f"/provider/{provider}"
                )
        # And add this authentication_router itself to the app.
        app.include_router(authentication_router, prefix="/api/auth")

    @lru_cache(1)
    def override_get_authenticators():
        return authenticators

    @lru_cache(1)
    def override_get_root_tree():
        return tree

    @lru_cache(1)
    def override_get_settings():
        settings = get_settings()
        for item in [
            "allow_anonymous_access",
            "secret_keys",
            "single_user_api_key",
            "access_token_max_age",
            "refresh_token_max_age",
            "session_max_age",
        ]:
            if authentication.get(item) is not None:
                setattr(settings, item, authentication[item])
        for item in ["allow_origins", "response_bytesize_limit"]:
            if server_settings.get(item) is not None:
                setattr(settings, item, server_settings[item])
        database = server_settings.get("database", {})
        if database.get("uri"):
            settings.database_uri = database["uri"]
        if database.get("pool_size"):
            settings.database_pool_size = database["pool_size"]
        if database.get("pool_pre_ping"):
            settings.database_pool_pre_ping = database["pool_pre_ping"]
        if database.get("max_overflow"):
            settings.database_max_overflow = database["max_overflow"]
        object_cache_available_bytes = server_settings.get("object_cache", {}).get(
            "available_bytes"
        )
        if object_cache_available_bytes is not None:
            setattr(
                settings, "object_cache_available_bytes", object_cache_available_bytes
            )
        if authentication.get("providers"):
            # If we support authentication providers, we need a database, so if one is
            # not set, use a SQLite database in the current working directory.
            settings.database_uri = settings.database_uri or "sqlite:///./tiled.sqlite"
        return settings

    @app.on_event("startup")
    async def startup_event():
        # Validate the single-user API key.
        settings = app.dependency_overrides[get_settings]()
        single_user_api_key = settings.single_user_api_key
        if single_user_api_key is not None:
            if not single_user_api_key.isalnum():
                raise ValueError(
                    """
    The API key must only contain alphanumeric characters. We enforce this because
    pasting other characters into a URL, as in ?api_key=..., can result in
    confusing behavior due to ambiguous encodings.

    The API key can be as long as you like. Here are two ways to generate a valid
    one:

    # With openssl:
    openssl rand -hex 32

    # With Python:
    python -c "import secrets; print(secrets.token_hex(32))"
    """
                )

        # Stash these to cancel this on shutdown.
        app.state.tasks = []
        # Trees and Authenticators can run tasks in the background.
        background_tasks = []
        background_tasks.extend(getattr(tree, "background_tasks", []))
        for authenticator in authenticators:
            background_tasks.extend(getattr(authenticator, "background_tasks", []))
        for task in background_tasks or []:
            asyncio_task = asyncio.create_task(task())
            app.state.tasks.append(asyncio_task)

        # The /search route is defined at server startup so that the user has the
        # opporunity to register custom query types before startup.
        app.include_router(declare_search_router(query_registry), prefix="/api")

        app.state.allow_origins.extend(settings.allow_origins)
        app.add_middleware(
            CORSMiddleware,
            allow_origins=app.state.allow_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        object_cache_logger.setLevel(settings.object_cache_log_level.upper())
        object_cache_available_bytes = settings.object_cache_available_bytes
        import psutil

        TOTAL_PHYSICAL_MEMORY = psutil.virtual_memory().total
        if object_cache_available_bytes < 0:
            raise ValueError("Negative object cache size is not interpretable.")
        if object_cache_available_bytes == 0:
            cache = NO_CACHE
            object_cache_logger.info("disabled")
        else:
            if 0 < object_cache_available_bytes < 1:
                # Interpret this as a fraction of system memory.

                object_cache_available_bytes = int(
                    TOTAL_PHYSICAL_MEMORY * object_cache_available_bytes
                )
            else:
                object_cache_available_bytes = int(object_cache_available_bytes)
            cache = ObjectCache(object_cache_available_bytes)
            percentage = round(
                object_cache_available_bytes / TOTAL_PHYSICAL_MEMORY * 100
            )
            object_cache_logger.info(
                f"Will use up to {object_cache_available_bytes:_} bytes ({percentage:d}% of total physical RAM)"
            )
        set_object_cache(cache)

        # Expose the root_tree here to make it easier to access it from tests,
        # in usages like:
        # client.context.app.state.root_tree
        app.state.root_tree = app.dependency_overrides[get_root_tree]()

        if settings.database_uri is not None:
            with get_sessionmaker(settings.database_settings)() as db:
                from ..database import orm
                from ..database.core import (
                    REQUIRED_REVISION,
                    DatabaseUpgradeNeeded,
                    UninitializedDatabase,
                    check_database,
                    initialize_database,
                    make_admin_by_identity,
                )

                engine = db.get_bind()
                redacted_url = engine.url._replace(password="[redacted]")
                try:
                    check_database(engine)
                except UninitializedDatabase:
                    # Create tables and stamp (alembic) revision.
                    logger.info(
                        f"Database {redacted_url} is new. "
                        f"Creating tables and marking revision {REQUIRED_REVISION}."
                    )
                    initialize_database(engine)
                    logger.info("Database initialized.")
                except DatabaseUpgradeNeeded as err:
                    print(
                        f"""

    The database used by Tiled to store authentication-related information
    was created using an older version of Tiled. It needs to be upgraded to
    work with this version of Tiled.

    Back up the database, and then run:

        tiled admin upgrade-database {redacted_url}
    """,
                        file=sys.stderr,
                    )
                    raise err from None
                else:
                    logger.info(f"Connected to existing database at {redacted_url}.")
                for admin in authentication.get("tiled_admins", []):
                    logger.info(
                        f"Ensuring that principal with identity {admin} has role 'admin'"
                    )
                    make_admin_by_identity(
                        db,
                        identity_provider=admin["provider"],
                        id=admin["id"],
                    )

            async def purge_expired_sessions_and_api_keys():
                logger.info("Purging expired Sessions and API keys from the database.")
                while True:
                    await asyncio.get_running_loop().run_in_executor(
                        None, purge_expired(engine, orm.Session)
                    )
                    await asyncio.get_running_loop().run_in_executor(
                        None, purge_expired(engine, orm.APIKey)
                    )
                    await asyncio.sleep(600)

            app.state.tasks.append(
                asyncio.create_task(purge_expired_sessions_and_api_keys())
            )

    @app.on_event("shutdown")
    async def shutdown_event():
        for task in app.state.tasks:
            task.cancel()

    app.add_middleware(
        CompressionMiddleware,
        compression_registry=compression_registry,
        minimum_size=1000,
    )

    @app.middleware("http")
    async def capture_metrics(request: Request, call_next):
        """
        Place metrics in Server-Timing header, in accordance with HTTP spec.
        """
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Server-Timing
        # https://w3c.github.io/server-timing/#the-server-timing-header-field
        # This information seems safe to share because the user can easily
        # estimate it based on request/response time, but if we add more detailed
        # information here we should keep in mind security concerns and perhaps
        # only include this for certain users.
        # Initialize a dict that routes and dependencies can stash metrics in.
        metrics = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))
        request.state.metrics = metrics
        # Record the overall application time.
        with record_timing(metrics, "app"):
            response = await call_next(request)
        response.__class__ = PatchedStreamingResponse  # tolerate memoryview
        # Server-Timing specifies times should be in milliseconds.
        # Prometheus specifies times should be in seconds.
        # Therefore, we store as seconds and convert to ms for Server-Timing here.
        # That is what the factor of 1000 below is doing.
        response.headers["Server-Timing"] = ", ".join(
            f"{key};"
            + ";".join(
                (
                    f"{metric}={value * 1000:.1f}"
                    if metric == "dur"
                    else f"{metric}={value:.1f}"
                )
                for metric, value in metrics_.items()
            )
            for key, metrics_ in metrics.items()
        )
        return response

    @app.middleware("http")
    async def double_submit_cookie_csrf_protection(request: Request, call_next):
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        csrf_cookie = request.cookies.get(CSRF_COOKIE_NAME)
        if (request.method not in SAFE_METHODS) and set(request.cookies).intersection(
            SENSITIVE_COOKIES
        ):
            if not csrf_cookie:
                return Response(
                    status_code=403, content="Expected tiled_csrf_token cookie"
                )
            # Get the token from the Header or (if not there) the query parameter.
            csrf_token = request.headers.get(CSRF_HEADER_NAME)
            if csrf_token is None:
                parsed_query = urllib.parse.parse_qs(request.url.query)
                csrf_token = parsed_query.get(CSRF_QUERY_PARAMETER)
            if not csrf_token:
                return Response(
                    status_code=403,
                    content=f"Expected {CSRF_QUERY_PARAMETER} query parameter or {CSRF_HEADER_NAME} header",
                )
            # Securely compare the token with the cookie.
            if not secrets.compare_digest(csrf_token, csrf_cookie):
                return Response(
                    status_code=403, content="Double-submit CSRF tokens do not match"
                )

        response = await call_next(request)
        response.__class__ = PatchedStreamingResponse  # tolerate memoryview
        if not csrf_cookie:
            response.set_cookie(
                key=CSRF_COOKIE_NAME,
                value=secrets.token_urlsafe(32),
                httponly=True,
                samesite="lax",
            )
        return response

    @app.middleware("http")
    async def set_cookies(request: Request, call_next):
        "This enables dependencies to inject cookies that they want to be set."
        # Create some Request state, to be (possibly) populated by dependencies.
        request.state.cookies_to_set = []
        response = await call_next(request)
        response.__class__ = PatchedStreamingResponse  # tolerate memoryview
        for params in request.state.cookies_to_set:
            params.setdefault("httponly", True)
            params.setdefault("samesite", "lax")
            response.set_cookie(**params)
        return response

    app.openapi = partial(custom_openapi, app)
    app.dependency_overrides[get_authenticators] = override_get_authenticators
    app.dependency_overrides[get_root_tree] = override_get_root_tree
    app.dependency_overrides[get_settings] = override_get_settings
    if query_registry is not None:

        @lru_cache(1)
        def override_get_query_registry():
            return query_registry

        app.dependency_overrides[get_query_registry] = override_get_query_registry
    if serialization_registry is not None:

        @lru_cache(1)
        def override_get_serialization_registry():
            return serialization_registry

        app.dependency_overrides[
            get_serialization_registry
        ] = override_get_serialization_registry

    if validation_registry is not None:

        @lru_cache(1)
        def override_get_validation_registry():
            return validation_registry

        app.dependency_overrides[
            get_validation_registry
        ] = override_get_validation_registry

    metrics_config = server_settings.get("metrics", {})
    if metrics_config.get("prometheus", False):
        from . import metrics

        app.include_router(metrics.router, prefix="/api")

        @app.middleware("http")
        async def capture_metrics_prometheus(request: Request, call_next):
            response = await call_next(request)
            response.__class__ = PatchedStreamingResponse  # tolerate memoryview
            metrics.capture_request_metrics(request, response)
            return response

    return app


def app_factory():
    """
    Return an ASGI app instance.

    Use a configuration file at the path specified by the environment variable
    TILED_CONFIG or, if unset, at the default path "./config.yml".

    This is intended to be used for horizontal deployment (using gunicorn, for
    example) where only a module and instance or factory can be specified.
    """
    config_path = os.getenv("TILED_CONFIG", "config.yml")
    logger.info(f"Using configuration from {Path(config_path).absolute()}")

    from ..config import construct_build_app_kwargs, parse_configs

    parsed_config = parse_configs(config_path)

    # This config was already validated when it was parsed. Do not re-validate.
    kwargs = construct_build_app_kwargs(parsed_config, source_filepath=config_path)
    web_app = build_app(**kwargs)
    uvicorn_config = parsed_config.get("uvicorn", {})
    print_admin_api_key_if_generated(
        web_app, host=uvicorn_config.get("host"), port=uvicorn_config.get("port")
    )
    return web_app


def __getattr__(name):
    """
    This supports tiled.server.app.app by creating app on demand.
    """
    if name == "app":
        try:
            return app_factory()
        except Exception as err:
            raise Exception("Failed to create app.") from err
    raise AttributeError(name)


def print_admin_api_key_if_generated(web_app, host, port):
    host = host or "127.0.0.1"
    port = port or 8000
    settings = web_app.dependency_overrides.get(get_settings, get_settings)()
    authenticators = web_app.dependency_overrides.get(
        get_authenticators, get_authenticators
    )()
    if settings.allow_anonymous_access:
        print(
            """
    Tiled server is running in "public" mode, permitting open, anonymous access
    for reading. Any data that is not specifically controlled with an access
    policy will be visible to anyone who can connect to this server.
""",
            file=sys.stderr,
        )
    if (not authenticators) and settings.single_user_api_key_generated:
        print(
            f"""
    Navigate a web browser to:

    http://{host}:{port}?api_key={settings.single_user_api_key}

    or connect a Tiled client to:

    http://{host}:{port}/api?api_key={settings.single_user_api_key}

""",
            file=sys.stderr,
        )
