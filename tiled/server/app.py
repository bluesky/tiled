import asyncio
import collections
from functools import lru_cache, partial
import os
import secrets
import sys
import urllib.parse

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from .authentication import (
    ACCESS_TOKEN_COOKIE_NAME,
    API_KEY_COOKIE_NAME,
    CSRF_COOKIE_NAME,
    REFRESH_TOKEN_COOKIE_NAME,
    password_authentication_router,
    external_authentication_router,
    get_authenticator,
)
from .compression import CompressionMiddleware
from .core import (
    get_root_tree,
    get_query_registry,
    get_serialization_registry,
    PatchedStreamingResponse,
    record_timing,
)
from .metrics import capture_request_metrics
from .object_cache import (
    ObjectCache,
    logger as object_cache_logger,
    NO_CACHE,
    set_object_cache,
)
from .router import declare_search_router, router
from .settings import get_settings
from ..media_type_registration import (
    compression_registry as default_compression_registry,
)


SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}
SENSITIVE_COOKIES = {
    API_KEY_COOKIE_NAME,
    ACCESS_TOKEN_COOKIE_NAME,
    REFRESH_TOKEN_COOKIE_NAME,
}
CSRF_HEADER_NAME = "x-csrf"
CSRF_QUERY_PARAMETER = "csrf"


def get_app(
    query_registry, compression_registry, include_routers=None, background_tasks=None
):
    """
    Construct an instance of the FastAPI application.

    Parameters
    ----------
    include_routers : list, optional
        List of additional FastAPI.Router objects to be included (merged) into the app
    background_tasks: list, optional
        List of async functions to be run on the event loop.
    """
    app = FastAPI()
    app.state.allow_origins = []
    app.include_router(router)
    for user_router in include_routers or []:
        app.include_router(user_router)

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

        authenticator = app.dependency_overrides[get_authenticator]()
        if authenticator is not None:
            if authenticator.handles_credentials:
                app.include_router(password_authentication_router)
            else:
                app.include_router(external_authentication_router)

        for task in background_tasks or []:
            asyncio.create_task(task())

        # The /search route is defined at server startup so that the user has the
        # opporunity to register custom query types before startup.
        app.include_router(declare_search_router(query_registry))

        app.state.allow_origins.extend(settings.allow_origins)

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
                f"Will use up to {object_cache_available_bytes} bytes ({percentage:d}% of total physical RAM)"
            )
        set_object_cache(cache)

    app.add_middleware(
        CompressionMiddleware,
        compression_registry=compression_registry,
        minimum_size=1000,
    )

    @app.middleware("http")
    async def capture_metrics(request: Request, call_next):
        """
        This does two things:

        - Place metrics in Server-Timing header, in accordance with HTTP spec.
        - Capture metrics in prometheus. The proemetheus metrics are availabe from /metrics.
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
        response.__class__ = PatchedStreamingResponse  # tolerate memoryview
        capture_request_metrics(request, response)
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

    app.add_middleware(
        CORSMiddleware,
        allow_origins=app.state.allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.openapi = partial(custom_openapi, app)
    return app


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


def serve_tree(
    tree,
    authentication=None,
    server_settings=None,
    query_registry=None,
    serialization_registry=None,
    compression_registry=None,
):
    """
    Serve a Tree

    Parameters
    ----------
    tree : Tree
    authentication: dict, optional
        Dict of authentication configuration.
    server_settings: dict, optional
        Dict of other server configuration.
    """
    authentication = authentication or {}
    server_settings = server_settings or {}
    authenticator = authentication.get("authenticator")

    @lru_cache(1)
    def override_get_authenticator():
        return authenticator

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
        for item in ["allow_origins"]:
            if server_settings.get(item) is not None:
                setattr(settings, item, server_settings[item])
        object_cache_available_bytes = server_settings.get("object_cache", {}).get(
            "available_bytes"
        )
        if object_cache_available_bytes is not None:
            setattr(
                settings,
                "object_cache_available_bytes",
                object_cache_available_bytes,
            )
        return settings

    # The Tree and Authenticator have the opportunity to add custom routes to
    # the server here. (Just for example, a Tree of BlueskyRuns uses this
    # hook to add a /documents route.) This has to be done before dependency_overrides
    # are processed, so we cannot just inject this configuration via Depends.
    include_routers = []
    include_routers.extend(getattr(tree, "include_routers", []))
    include_routers.extend(getattr(authenticator, "include_routers", []))
    # Likewise, the Tree and Authenticator can run tasks in the background.
    # These typically contain a periodic loop.
    background_tasks = []
    background_tasks.extend(getattr(tree, "background_tasks", []))
    background_tasks.extend(getattr(authenticator, "background_tasks", []))
    app = get_app(
        query_registry or get_query_registry(),
        compression_registry or default_compression_registry,
        include_routers=include_routers,
        background_tasks=background_tasks,
    )
    app.dependency_overrides[get_authenticator] = override_get_authenticator
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

    from ..config import construct_serve_tree_kwargs, parse_configs

    parsed_config = parse_configs(config_path)

    # This config was already validated when it was parsed. Do not re-validate.
    kwargs = construct_serve_tree_kwargs(parsed_config, source_filepath=config_path)
    web_app = serve_tree(**kwargs)
    uvicorn_config = parsed_config.get("uvicorn", {})
    print_admin_api_key_if_generated(
        web_app, host=uvicorn_config["host"], port=uvicorn_config["port"]
    )
    return web_app


def __getattr__(name):
    """
    This supports tiled.server.app.app by creating app on demand.
    """
    if name == "app":
        return app_factory()
    raise AttributeError(name)


def print_admin_api_key_if_generated(web_app, host, port):
    host = host or "127.0.0.1"
    port = port or 8000
    settings = web_app.dependency_overrides.get(get_settings, get_settings)()
    authenticator = web_app.dependency_overrides.get(
        get_authenticator, get_authenticator
    )()
    if settings.allow_anonymous_access:
        print(
            """
    Tiled server is running in "public" mode, permitting open, anonymous access.
    Any data that is not specifically controlled with an access policy
    will be visible to anyone who can connect to this server.
""",
            file=sys.stderr,
        )
    elif (authenticator is None) and settings.single_user_api_key_generated:
        print(
            f"""
    Use the following URL to connect to Tiled:

    "http://{host}:{port}?api_key={settings.single_user_api_key}"
""",
            file=sys.stderr,
        )
