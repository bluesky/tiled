from functools import lru_cache
import secrets
import sys
import time
import urllib.parse

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from .authentication import (
    API_KEY_COOKIE_NAME,
    CSRF_COOKIE_NAME,
    authentication_router,
    get_authenticator,
)
from .core import get_root_catalog, PatchedStreamingResponse
from .router import declare_search_router, router
from .settings import get_settings


SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}
SENSITIVE_COOKIES = {API_KEY_COOKIE_NAME}
CSRF_HEADER_NAME = "x-csrf"
CSRF_QUERY_PARAMETER = "csrf"


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
    async def double_submit_cookie_csrf_protection(request: Request, call_next):
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        csrf_cookie = request.cookies.get(CSRF_COOKIE_NAME)
        if (request.method not in SAFE_METHODS) and set(request.cookies).intersection(
            SENSITIVE_COOKIES
        ):
            if not csrf_cookie:
                raise HTTPException(
                    status_code=403, detail="Expected tiled_csrf_token cookie"
                )
            # Get the token from the Header or (if not there) the query parameter.
            csrf_token = request.headers.get(CSRF_HEADER_NAME)
            if csrf_token is None:
                parsed_query = urllib.parse.parse_qs(request.url.query)
                csrf_token = parsed_query.get(CSRF_QUERY_PARAMETER)
            if not csrf_token:
                raise HTTPException(
                    status_code=403,
                    detail="Expected csrf_token query parameter or x-tiled-csrf-token header",
                )
            # Securely compare the token with the cookie.
            if not secrets.compare_digest(csrf_token, csrf_cookie):
                raise HTTPException(
                    status_code=403, detail="Double-submit CSRF tokens do not match"
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
        for key, value in request.state.cookies_to_set:
            response.set_cookie(
                key=key,
                value=value,
                httponly=True,
                samesite="lax",
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
    catalog,
    authentication=None,
):
    """
    Serve a Catalog

    Parameters
    ----------
    catalog : Catalog
    authentication: dict, optional
        Dict of authentication configuration.
    """
    authentication = authentication or {}
    authenticator = authentication.get("authenticator")

    @lru_cache(1)
    def override_get_authenticator():
        return authenticator

    @lru_cache(1)
    def override_get_root_catalog():
        return catalog

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
        return settings

    # The Catalog and Authenticator have the opportunity to add custom routes to
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
