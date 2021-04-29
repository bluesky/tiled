from functools import lru_cache
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from .settings import get_settings
from .router import declare_search_router, router
from .core import PatchedStreamingResponse
from .authentication import authentication_router


def get_app(include_routers=None):
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

    app.add_middleware(
        CORSMiddleware,
        allow_origins=get_settings().allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return app


def serve_catalog(catalog, authenticator=None):
    @lru_cache(1)
    def override_settings():
        settings = get_settings()
        settings.catalog = catalog
        settings.authenticator = authenticator
        return settings

    # The Catalog and Authenticator have the opporunity to add custom routes to
    # the server here. (Just for example, a Catalog of BlueskyRuns uses this
    # hook to add a /documents route.) This has to be done before dependency_overrides
    # are processed, so we cannot just use get_settings to inject this configuration.
    include_routers = []
    include_routers.extend(getattr(catalog, "include_routers", []))
    include_routers.extend(getattr(authenticator, "include_routers", []))
    app = get_app(include_routers=include_routers)
    app.dependency_overrides[get_settings] = override_settings
    return app


if __name__ == "__main__":
    import uvicorn

    app = get_app()
    uvicorn.run(app, host="0.0.0.0", port=8000)
