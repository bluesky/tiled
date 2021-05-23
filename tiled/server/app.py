from functools import lru_cache
import secrets
import sys
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from .authentication import authentication_router, get_authenticator
from .core import get_root_catalog, PatchedStreamingResponse
from .router import declare_search_router, router
from .settings import get_settings


def get_app(include_routers=None):
    """
    Construct an instance of the FastAPI application.

    Parameters
    ----------
    include_routers : list, optional
        List of additional FastAPI.Router objects to be included (merged) into the app
    """
    app = FastAPI()
    app.include_router(router)
    app.include_router(authentication_router)

    for user_router in include_routers or []:
        app.include_router(user_router)

    @app.on_event("startup")
    async def startup_event():
        # The /search route is defined at server startup so that the user has the
        # opporunity to register custom query types before startup.
        app.include_router(declare_search_router())

    @app.middleware("http")
    async def add_server_timing_header(request: Request, call_next):
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Server-Timing
        # https://w3c.github.io/server-timing/#the-server-timing-header-field
        # This information seems safe to share because the user can easily
        # estimate it based on request/response time, but if we add more detailed
        # information here we should keep in mind security concerns and perhaps
        # only include this for certain users.
        # Units are ms.
        start_time = time.perf_counter()
        response = await call_next(request)
        response.__class__ = PatchedStreamingResponse  # tolerate memoryview
        process_time = time.perf_counter() - start_time
        response.headers["Server-Timing"] = f"app;dur={1000 * process_time:.1f}"
        return response

    @app.middleware("http")
    async def set_api_key_cookie(request: Request, call_next):
        # If the API key is provided via a header or query, set it as a cookie.
        response = await call_next(request)
        response.__class__ = PatchedStreamingResponse  # tolerate memoryview
        if ("X-TILED-API-KEY" in request.headers) and (response.status_code < 400):
            response.set_cookie(
                key="TILED_API_KEY",
                value=request.headers["X-TILED-API-KEY"],
                domain=request.url.hostname,
            )
            response.set_cookie(
                key="TILED_CSRF_TOKEN",
                value=secrets.token_hex(32),
                domain=request.url.hostname,
            )
        elif ("api_key" in request.url.query) and (response.status_code < 400):
            params = request.url.query.split("&")
            for item in params:
                if "=" in item:
                    key, value = item.split("=")
                    if key == "api_key":
                        response.set_cookie(
                            key="TILED_API_KEY",
                            value=value,
                            domain=request.url.hostname,
                        )
            response.set_cookie(
                key="TILED_CSRF_TOKEN",
                value=secrets.token_hex(32),
                domain=request.url.hostname,
            )
        return response

    app.add_middleware(
        CORSMiddleware,
        allow_origins=get_settings().allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return app


def serve_catalog(
    catalog, authenticator=None, allow_anonymous_access=None, secret_keys=None
):
    """
    Serve a Catalog

    Parameters
    ----------
    catalog : Catalog
    authenticator : Authenticator, optional
    allow_anonymous_access : bool, optional
        Default is False.
    secret_keys : List[str], optional
        This list may contain one or more keys.
        The first key is used for *encoding*. All keys are tried for *decoding*
        until one works or they all fail. This supports key rotation.
        If None, a secure secret is generated.
    """

    @lru_cache(1)
    def override_get_authenticator():
        return authenticator

    @lru_cache(1)
    def override_get_root_catalog():
        return catalog

    @lru_cache(1)
    def override_get_settings():
        settings = get_settings()
        if allow_anonymous_access is not None:
            settings.allow_anonymous_access = allow_anonymous_access
        if secret_keys is not None:
            settings.secret_keys = secret_keys
        return settings

    # The Catalog and Authenticator have the opporunity to add custom routes to
    # the server here. (Just for example, a Catalog of BlueskyRuns uses this
    # hook to add a /documents route.) This has to be done before dependency_overrides
    # are processed, so we cannot just inject this configuration via Depends.
    include_routers = []
    include_routers.extend(getattr(catalog, "include_routers", []))
    include_routers.extend(getattr(authenticator, "include_routers", []))
    app = get_app(include_routers=include_routers)
    app.dependency_overrides[get_authenticator] = override_get_authenticator
    app.dependency_overrides[get_root_catalog] = override_get_root_catalog
    app.dependency_overrides[get_settings] = override_get_settings
    return app


def print_admin_api_key_if_generated(web_app):
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

    "http://127.0.0.1:8000?api_key={settings.single_user_api_key}"
""",
            file=sys.stderr,
        )
