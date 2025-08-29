import asyncio
import collections
import contextvars
import logging
import os
import secrets
import sys
import urllib.parse
import warnings
from contextlib import asynccontextmanager
from functools import cache, partial
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable, Coroutine, Optional, TypedDict, Union

import anyio
import packaging.version
import yaml
from asgi_correlation_id import CorrelationIdMiddleware, correlation_id
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.exception_handlers import http_exception_handler
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import FileResponse
from starlette.status import (
    HTTP_200_OK,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from tiled.query_registration import QueryRegistry, default_query_registry
from tiled.server.protocols import ExternalAuthenticator, InternalAuthenticator

from ..catalog.adapter import WouldDeleteData
from ..config import construct_build_app_kwargs, parse_configs
from ..media_type_registration import (
    CompressionRegistry,
    SerializationRegistry,
    default_compression_registry,
    default_deserialization_registry,
    default_serialization_registry,
)
from ..utils import SHARE_TILED_PATH, Conflicts, UnsupportedQueryType
from ..validation_registration import ValidationRegistry, default_validation_registry
from .compression import CompressionMiddleware
from .router import get_router
from .settings import Settings, get_settings
from .utils import API_KEY_COOKIE_NAME, CSRF_COOKIE_NAME, get_root_url, record_timing
from .zarr import get_zarr_router_v2, get_zarr_router_v3

SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}
SENSITIVE_COOKIES = {
    API_KEY_COOKIE_NAME,
}
CSRF_HEADER_NAME = "x-csrf"
CSRF_QUERY_PARAMETER = "csrf"

MINIMUM_SUPPORTED_PYTHON_CLIENT_VERSION = packaging.version.parse("0.1.0a104")

logger = logging.getLogger(__name__)
logger.setLevel("INFO")
handler = logging.StreamHandler()
handler.setLevel("DEBUG")
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)


# This is used to pass the currently-authenticated principal into the logger.
current_principal = contextvars.ContextVar("current_principal")


AppTask = Callable[[], Coroutine[None, None, Any]]
"""Async function to be run as part of the app's lifecycle"""


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
    query_registry: Optional[QueryRegistry] = None,
    serialization_registry: Optional[SerializationRegistry] = None,
    deserialization_registry: Optional[SerializationRegistry] = None,
    compression_registry: Optional[CompressionRegistry] = None,
    validation_registry: Optional[ValidationRegistry] = None,
    tasks: Optional[dict[str, list[AppTask]]] = None,
    scalable=False,
    access_policy=None,
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
    access_policy:
        AccessPolicy object encoding rules for which users can see which entries.
    """
    authentication = authentication or {}
    authenticators: dict[str, Union[ExternalAuthenticator, InternalAuthenticator]] = {
        spec["provider"]: spec["authenticator"]
        for spec in authentication.get("providers", [])
    }
    server_settings = server_settings or {}
    query_registry = query_registry or default_query_registry
    serialization_registry = serialization_registry or default_serialization_registry
    deserialization_registry = (
        deserialization_registry or default_deserialization_registry
    )
    compression_registry = compression_registry or default_compression_registry
    validation_registry = validation_registry or default_validation_registry

    tasks: TaskMap = {
        "startup": (tasks or {}).get("startup", []),
        "background": (tasks or {}).get("background", []),
        "shutdown": (tasks or {}).get("shutdown", []),
    }
    # The tasks are collected at config-parsing time off of the sub-trees.
    # Collect the tasks off the root tree here, so that it works when
    # a single tree is passed to build_app(...) directly, as happens in the tests.
    tasks["startup"].extend(getattr(tree, "startup_tasks", []))
    tasks["background"].extend(getattr(tree, "background_tasks", []))
    tasks["shutdown"].extend(getattr(tree, "shutdown_tasks", []))

    if scalable:
        if authentication.get("providers"):
            # Even if the deployment allows public, anonymous access, secret
            # keys are needed to generate JWTs for any users that do log in.
            if (
                "secret_keys" not in authentication
                and "TILED_SECRET_KEYS" not in os.environ
            ):
                raise UnscalableConfig(
                    dedent(
                        """
                        In a scaled (multi-process) deployment, when Tiled is configured with an
                        Authenticator, secret keys must be provided via configuration like

                        authentication:
                          secret_keys:
                            - SECRET
                          ...

                        or via the environment variable TILED_SECRET_KEYS.\
                        """,
                    )
                )
            # Multi-user authentication requires a database. We cannot fall
            # back to the default of an in-memory SQLite database in a
            # horizontally scaled deployment.
            if not server_settings.get("database", {}).get("uri"):
                raise UnscalableConfig(
                    dedent(
                        """
                        In a scaled (multi-process) deployment, when Tiled is configured with an
                        Authenticator, a persistent database must be provided via configuration like

                        database:
                          uri: sqlite:////path/to/database.sqlite

                        """
                    )
                )
        else:
            # No authentication provider is configured, so no secret keys are
            # needed, but a single-user API key must be set.
            if not (
                ("single_user_api_key" in authentication)
                or ("TILED_SINGLE_USER_API_KEY" in os.environ)
            ):
                raise UnscalableConfig(
                    dedent(
                        """
                        In a scaled (multi-process) deployment, when Tiled is configured for
                        single-user access (i.e. without an Authenticator) a single-user API key must
                        be provided via configuration like

                        authentication:
                          single_user_api_key: SECRET
                          ...

                        or via the environment variable TILED_SINGLE_USER_API_KEY.\
                        """,
                    )
                )

    # If we reach here, the no configuration problems were found.

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        "Manage lifespan events for each event loop that the app runs in"
        await startup_event()
        yield
        await shutdown_event()

    app = FastAPI(lifespan=lifespan)

    # Healthcheck for deployment to containerized systems, needs to preempt other responses.
    # Standardized for Kubernetes, but also used by other systems.
    @app.get("/healthz", status_code=200)
    async def healthz():
        return {"status": "ready"}

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
                raise HTTPException(status_code=HTTP_401_UNAUTHORIZED)
            except FileNotFoundError:
                # This may be a URL that has meaning to the client-side application,
                # such as /ui//metadata/a/b/c.
                # Serve index.html and let the client-side application sort it out.
                if try_app:
                    response = await lookup_file("index.html", try_app=False)
                    return response
                raise HTTPException(status_code=HTTP_404_NOT_FOUND)
            except OSError:
                raise
            return FileResponse(
                full_path,
                stat_result=stat_result,
                status_code=HTTP_200_OK,
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
        ):
            return templates.TemplateResponse(
                request,
                "index.html",
                {
                    # This is used to construct the link to the React UI.
                    "root_url": get_root_url(request),
                    # If defined, this adds a Binder link to the page.
                    "binder_link": os.getenv("TILED_BINDER_LINK"),
                },
            )

        TILED_UI_SETTINGS = os.getenv("TILED_UI_SETTINGS")
        if TILED_UI_SETTINGS is None:
            TILED_UI_SETTINGS = Path(
                SHARE_TILED_PATH, "static", "default_ui_settings.yml"
            )
        if TILED_UI_SETTINGS != "":
            # If "", the settings are being served some other way, such as by
            # nginx, perhaps because the API is served from a sub-path of this netloc.

            # The settings are YAML-formatted because that is more readable and supports
            # comments. But they are served as JSON because that is easy to deal with
            # on the client side.
            ui_settings = yaml.safe_load(Path(TILED_UI_SETTINGS).read_text())
            if root_path := server_settings.get("root_path", ""):
                ui_settings["api_url"] = f"{root_path}{ui_settings['api_url']}"

            @app.get("/tiled-ui-settings")
            async def tiled_ui_settings():
                return ui_settings

    @app.exception_handler(Conflicts)
    async def conflicts_exception_handler(request: Request, exc: Conflicts):
        message = exc.args[0]
        return JSONResponse(status_code=HTTP_409_CONFLICT, content={"detail": message})

    @app.exception_handler(UnsupportedQueryType)
    async def unsupported_query_type_exception_handler(
        request: Request, exc: UnsupportedQueryType
    ):
        query_type = exc.args[0]
        return JSONResponse(
            status_code=HTTP_400_BAD_REQUEST,
            content={
                "detail": f"The query type {query_type!r} is not supported on this node."
            },
        )

    @app.exception_handler(WouldDeleteData)
    async def would_delete_data_exception_handler(
        request: Request, exc: WouldDeleteData
    ):
        return JSONResponse(
            status_code=HTTP_409_CONFLICT,
            content={"detail": exc.args[0]},
        )

    # This list will be mutated when settings are processed at app startup.
    app.state.allow_origins = []
    app.add_middleware(
        CORSMiddleware,
        allow_origins=app.state.allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        # If we restrict the allowed_headers in future, remember to include
        # exemptions for these, related to asgi_correlation_id.
        # allow_headers=["X-Requested-With", "X-Tiled-Request-ID"],
        expose_headers=["X-Tiled-Request-ID"],
    )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        return await http_exception_handler(
            request,
            HTTPException(
                HTTP_500_INTERNAL_SERVER_ERROR,
                "Internal server error",
                headers={"X-Tiled-Request-ID": correlation_id.get() or ""},
            ),
        )

    router = get_router(
        query_registry,
        serialization_registry,
        deserialization_registry,
        validation_registry,
        authenticators,
    )
    app.include_router(router, prefix="/api/v1")

    app.include_router(get_zarr_router_v2(), prefix="/zarr/v2")
    app.include_router(get_zarr_router_v3(), prefix="/zarr/v3")

    # The Tree and Authenticator have the opportunity to add custom routes to
    # the server here. (Just for example, a Tree of BlueskyRuns uses this
    # hook to add a /documents route.) This has to be done before dependency_overrides
    # are processed, so we cannot just inject this configuration via Depends.
    for custom_router in getattr(tree, "include_routers", []):
        app.include_router(custom_router, prefix="/api/v1")

    app.state.access_policy = access_policy

    if authentication.get("providers", []):
        # Delay this imports to avoid delaying startup with the SQL and cryptography
        # imports if they are not needed.
        from .authentication import (
            add_external_routes,
            add_internal_routes,
            authentication_router,
            oauth2_scheme,
        )

        # For the OpenAPI schema, inject a OAuth2PasswordBearer URL.
        first_provider = authentication["providers"][0]["provider"]
        oauth2_scheme.model.flows.password.tokenUrl = (
            f"/api/v1/auth/provider/{first_provider}/token"
        )
        # Authenticators provide Router(s) for their particular flow.
        # Collect them in the authentication_router.
        authentication_router = authentication_router()
        # This adds the universal routes like /session/refresh and /session/revoke.
        # Below we will add routes specific to our authentication providers.

        for spec in authentication["providers"]:
            provider = spec["provider"]
            authenticator = spec["authenticator"]
            if isinstance(authenticator, InternalAuthenticator):
                add_internal_routes(authentication_router, provider, authenticator)
            elif isinstance(authenticator, ExternalAuthenticator):
                add_external_routes(authentication_router, provider, authenticator)
            else:
                raise ValueError(f"unknown authenticator type {type(authenticator)}")
            for custom_router in getattr(authenticator, "include_routers", []):
                authentication_router.include_router(
                    custom_router, prefix=f"/provider/{provider}"
                )
        # And add this authentication_router itself to the app.
        app.include_router(authentication_router, prefix="/api/v1/auth")
        app.state.authenticated = True
    else:
        app.state.authenticated = False

    # Expose the root_tree here to make it accessible from endpoints via
    # request.app.state and in tests via client.context.app.state
    app.state.root_tree = tree

    @cache
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
        for item in [
            "allow_origins",
            "response_bytesize_limit",
            "exact_count_limit",
            "reject_undeclared_specs",
            "expose_raw_assets",
        ]:
            if server_settings.get(item) is not None:
                setattr(settings, item, server_settings[item])
        database = server_settings.get("database", {})
        if uri := database.get("uri"):
            settings.database_settings.uri = uri
        if pool_size := database.get("pool_size"):
            settings.database_settings.pool_size = pool_size
        if pool_pre_ping := database.get("pool_pre_ping"):
            settings.database_settings.pool_pre_ping = pool_pre_ping
        if max_overflow := database.get("max_overflow"):
            settings.database_settings.max_overflow = max_overflow
        if init_if_not_exists := database.get("init_if_not_exists"):
            settings.database_init_if_not_exists = init_if_not_exists
        if authentication.get("providers"):
            # If we support authentication providers, we need a database, so if one is
            # not set, use a SQLite database in memory. Horizontally scaled deployments
            # must specify a persistent database.
            settings.database_settings.uri = (
                settings.database_settings.uri or "sqlite://"
            )
        return settings

    async def startup_event():
        from .. import __version__

        logger.info(f"Tiled version {__version__}")
        # Validate the single-user API key.
        settings: Settings = app.dependency_overrides[get_settings]()
        single_user_api_key = settings.single_user_api_key
        API_KEY_MSG = dedent(
            """
            Here are two ways to generate a good API key:

            # With openssl:
            openssl rand -hex 32

            # With Python:
            python -c "import secrets; print(secrets.token_hex(32))"

            """
        )
        if single_user_api_key is not None:
            if not single_user_api_key:
                raise ValueError(
                    dedent(
                        """
                        The single-user API key is set to an empty value. Perhaps the environment
                        variable TILED_SINGLE_USER_API_KEY is set to an empty string.
                        """
                        + API_KEY_MSG
                    )
                )
            if not single_user_api_key.isalnum():
                raise ValueError(
                    dedent(
                        """
                        The API key must only contain alphanumeric characters. We enforce this because
                        pasting other characters into a URL, as in ?api_key=..., can result in
                        confusing behavior due to ambiguous encodings.
                        """
                    )
                    + API_KEY_MSG
                )

        # Run startup tasks collected from trees (adapters).
        for task in tasks["startup"]:
            await task()

        # Stash these to cancel this on shutdown.
        app.state.tasks = []
        # Trees and Authenticators can run tasks in the background.
        background_tasks: list[AppTask] = []
        background_tasks.extend(tasks["background"])
        for authenticator in authenticators:
            background_tasks.extend(getattr(authenticator, "background_tasks", []))
        for task in background_tasks:
            asyncio_task = asyncio.create_task(task())
            app.state.tasks.append(asyncio_task)

        app.state.allow_origins.extend(settings.allow_origins)

        if settings.database_settings.uri is not None:
            from sqlalchemy.ext.asyncio import AsyncSession

            from ..alembic_utils import (
                DatabaseUpgradeNeeded,
                UninitializedDatabase,
                check_database,
            )
            from ..authn_database import orm
            from ..authn_database.connection_pool import open_database_connection_pool
            from ..authn_database.core import (
                ALL_REVISIONS,
                REQUIRED_REVISION,
                initialize_database,
                make_admin_by_identity,
                purge_expired,
            )

            # This creates a connection pool and stashes it in a module-global
            # registry, keyed on database_settings, where can be retrieved by
            # the Dependency get_database_session.
            engine = open_database_connection_pool(settings.database_settings)
            if not engine.url.database or engine.url.query.get("mode") == "memory":
                # Special-case for in-memory SQLite: Because it is transient we can
                # skip over anything related to migrations.
                await initialize_database(engine)
                logger.info("Transient in-memory database initialized.")
            else:
                redacted_url = engine.url._replace(password="[redacted]")
                try:
                    await check_database(engine, REQUIRED_REVISION, ALL_REVISIONS)
                except UninitializedDatabase:
                    if settings.database_init_if_not_exists:
                        # The alembic stamping can only be does synchronously.
                        # The cleanest option available is to start a subprocess
                        # because SQLite is allergic to threads.
                        import subprocess

                        # TODO Check if catalog exists.
                        subprocess.run(
                            [
                                sys.executable,
                                "-m",
                                "tiled",
                                "admin",
                                "initialize-database",
                                str(engine.url),
                            ],
                            capture_output=True,
                            check=True,
                        )
                    else:
                        print(
                            dedent(
                                f"""

                                No database found at {redacted_url}

                                To create one, run:

                                    tiled admin init-database {redacted_url}
                                """
                            ),
                            file=sys.stderr,
                        )
                        raise
                except DatabaseUpgradeNeeded as err:
                    print(
                        dedent(
                            f"""

                            The database used by Tiled to store authentication-related information
                            was created using an older version of Tiled. It needs to be upgraded to
                            work with this version of Tiled.

                            Back up the database, and then run:

                                tiled admin upgrade-database {redacted_url}
                            """
                        ),
                        file=sys.stderr,
                    )
                    raise err from None
                else:
                    logger.info(f"Connected to existing database at {redacted_url}.")
            for admin in authentication.get("tiled_admins", []):
                logger.info(
                    f"Ensuring that principal with identity {admin} has role 'admin'"
                )
                async with AsyncSession(
                    engine, autoflush=False, expire_on_commit=False
                ) as session:
                    await make_admin_by_identity(
                        session,
                        identity_provider=admin["provider"],
                        id=admin["id"],
                    )

            if app.state.access_policy is not None and hasattr(
                app.state.access_policy, "access_tags_parser"
            ):
                await app.state.access_policy.access_tags_parser.connect()

            async def purge_expired_sessions_and_api_keys():
                PURGE_INTERVAL = 600  # seconds
                while True:
                    async with AsyncSession(
                        engine, autoflush=False, expire_on_commit=False
                    ) as db_session:
                        num_expired_sessions = await purge_expired(
                            db_session, orm.Session
                        )
                        if num_expired_sessions:
                            logger.info(
                                f"Purged {num_expired_sessions} expired Sessions from the database."
                            )
                        num_expired_api_keys = await purge_expired(
                            db_session, orm.APIKey
                        )
                        if num_expired_api_keys:
                            logger.info(
                                f"Purged {num_expired_api_keys} expired API keys from the database."
                            )
                    await asyncio.sleep(PURGE_INTERVAL)

            app.state.tasks.append(
                asyncio.create_task(purge_expired_sessions_and_api_keys())
            )

    async def shutdown_event():
        # Run shutdown tasks collected from trees (adapters).
        for task in tasks["shutdown"]:
            await task()

        settings: Settings = app.dependency_overrides[get_settings]()
        if settings.database_settings.uri is not None:
            from ..authn_database.connection_pool import close_database_connection_pool

            await close_database_connection_pool(settings.database_settings)
        for task in app.state.tasks:
            task.cancel()

    app.add_middleware(
        CompressionMiddleware,
        compression_registry=compression_registry,
        minimum_size=1000,
    )

    @app.middleware("http")
    async def double_submit_cookie_csrf_protection(
        request: Request, call_next: RequestResponseEndpoint
    ):
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        csrf_cookie = request.cookies.get(CSRF_COOKIE_NAME)
        if (request.method not in SAFE_METHODS) and set(request.cookies).intersection(
            SENSITIVE_COOKIES
        ):
            if not csrf_cookie:
                return Response(
                    status_code=HTTP_403_FORBIDDEN,
                    content="Expected tiled_csrf_token cookie",
                )
            # Get the token from the Header or (if not there) the query parameter.
            csrf_token = request.headers.get(CSRF_HEADER_NAME)
            if csrf_token is None:
                parsed_query = urllib.parse.parse_qs(request.url.query)
                csrf_token = parsed_query.get(CSRF_QUERY_PARAMETER)
            if not csrf_token:
                return Response(
                    status_code=HTTP_403_FORBIDDEN,
                    content=f"Expected {CSRF_QUERY_PARAMETER} query parameter or {CSRF_HEADER_NAME} header",
                )
            # Securely compare the token with the cookie.
            if not secrets.compare_digest(csrf_token, csrf_cookie):
                return Response(
                    status_code=HTTP_403_FORBIDDEN,
                    content="Double-submit CSRF tokens do not match",
                )

        response = await call_next(request)
        if not csrf_cookie:
            response.set_cookie(
                key=CSRF_COOKIE_NAME,
                value=secrets.token_urlsafe(32),
                httponly=True,
                samesite="lax",
            )
        return response

    @app.middleware("http")
    async def client_compatibility_check(
        request: Request, call_next: RequestResponseEndpoint
    ):
        user_agent = request.headers.get("user-agent", "")
        if user_agent.startswith("python-tiled/"):
            agent, _, raw_version = user_agent.partition("/")
            try:
                parsed_version = packaging.version.parse(raw_version)
            except Exception as caught_exception:
                invalid_version_message = (
                    f"Python Tiled client version is reported as {raw_version}. "
                    "This cannot be parsed as a valid version."
                )
                logger.warning(invalid_version_message)
                if isinstance(caught_exception, packaging.version.InvalidVersion):
                    warnings.warn(invalid_version_message)
            else:
                if (not parsed_version.is_devrelease) and (
                    parsed_version < MINIMUM_SUPPORTED_PYTHON_CLIENT_VERSION
                ):
                    return JSONResponse(
                        status_code=HTTP_400_BAD_REQUEST,
                        content={
                            "detail": (
                                f"Python Tiled client reports version {parsed_version}. "
                                f"Version {MINIMUM_SUPPORTED_PYTHON_CLIENT_VERSION} or higher "
                                "is needed to communicate with this Tiled server."
                            ),
                        },
                    )
        response = await call_next(request)
        return response

    @app.middleware("http")
    async def set_cookies(request: Request, call_next: RequestResponseEndpoint):
        "This enables dependencies to inject cookies that they want to be set."
        # Create some Request state, to be (possibly) populated by dependencies.
        request.state.cookies_to_set = []
        response = await call_next(request)
        for params in request.state.cookies_to_set:
            params.setdefault("httponly", True)
            params.setdefault("samesite", "lax")
            response.set_cookie(**params)
        return response

    app.openapi = partial(custom_openapi, app)
    app.dependency_overrides[get_settings] = override_get_settings

    @app.middleware("http")
    async def capture_metrics(request: Request, call_next: RequestResponseEndpoint):
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

    metrics_config = server_settings.get("metrics", {})
    if metrics_config.get("prometheus", True):
        # PROMETHEUS_MULTIPROC_DIR puts prometheus_client in multiprocess mode
        # (for e.g. gunicorn) which uses a directory of memory-mapped files.
        # If that environment variable is set, check that the directory exists
        # and is writable.
        prometheus_multiproc_dir = os.getenv("PROMETHEUS_MULTIPROC_DIR", None)
        if prometheus_multiproc_dir:
            if not Path(prometheus_multiproc_dir).is_dir():
                raise ValueError(
                    "prometheus enabled and PROMETHEUS_MULTIPROC_DIR is set but "
                    f"({prometheus_multiproc_dir}) is not a directory"
                )
            if not os.access(prometheus_multiproc_dir, os.W_OK):
                raise ValueError(
                    "prometheus enabled and PROMETHEUS_MULTIPROC_DIR is set but "
                    f"({prometheus_multiproc_dir}) is not writable"
                )

        from . import metrics

        app.include_router(metrics.router, prefix="/api/v1")

        @app.middleware("http")
        async def capture_metrics_prometheus(
            request: Request, call_next: RequestResponseEndpoint
        ):
            try:
                response = await call_next(request)
            except Exception:
                # Make an ephemeral response solely for 'capture_request_metrics'.
                # It will only be used in the 'finally' clean-up block.
                only_for_metrics = Response(status_code=HTTP_500_INTERNAL_SERVER_ERROR)
                response = only_for_metrics
                # Now re-raise the exception so that the server can generate and
                # send an appropriate response to the client.
                raise
            finally:
                metrics.capture_request_metrics(request, response)

            # This is a *real* response (i.e., not the 'only_for_metrics' response).
            # An exception above would have triggered an early exit.
            return response

    @app.middleware("http")
    async def current_principal_logging_filter(
        request: Request, call_next: RequestResponseEndpoint
    ):
        # Set a default, which may be overridden below by authentication code.
        request.state.principal = None
        response = await call_next(request)
        current_principal.set(request.state.principal)
        return response

    app.add_middleware(
        CorrelationIdMiddleware,
        header_name="X-Tiled-Request-ID",
        generator=lambda: secrets.token_hex(8),
    )

    return app


def build_app_from_config(config, source_filepath=None, scalable=False):
    "Convenience function that calls build_app(...) given config as dict."
    kwargs = construct_build_app_kwargs(config, source_filepath=source_filepath)
    return build_app(scalable=scalable, **kwargs)


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

    parsed_config = parse_configs(config_path)

    # This config was already validated when it was parsed. Do not re-validate.
    web_app = build_app_from_config(parsed_config, config_path)
    uvicorn_config = parsed_config.get("uvicorn", {})
    print_server_info(
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


def print_server_info(
    web_app: FastAPI,
    host: str = "127.0.0.1",
    port: int = 8000,
    include_api_key: bool = False,
):
    settings = get_settings()

    if settings.allow_anonymous_access:
        print(
            """
    Tiled server is running in "public" mode, permitting open, anonymous access
    for reading. Any data that is not specifically controlled with an access
    policy will be visible to anyone who can connect to this server.
""",
            file=sys.stderr,
        )
    if not web_app.state.authenticated and include_api_key:
        print(
            f"""
    Navigate a web browser or connect a Tiled client to:

    http://{host}:{port}?api_key={settings.single_user_api_key}

""",
            file=sys.stderr,
        )
    if settings.allow_anonymous_access:
        print(
            """    Because this server is public, the '?api_key=...' portion of
    the URL is needed only for _writing_ data (if applicable).

""",
            file=sys.stderr,
        )


class UnscalableConfig(Exception):
    pass


class TaskMap(TypedDict):
    background: list[AppTask]
    startup: list[AppTask]
    shutdown: list[AppTask]
