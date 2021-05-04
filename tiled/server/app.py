from functools import lru_cache
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from .authentication import authentication_router, get_authenticator
from .core import get_root_catalog, PatchedStreamingResponse
from .router import declare_search_router, router
from .settings import get_settings
from ..catalogs.in_memory import Catalog


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


def serve_catalogs(catalogs, authenticator=None):
    """
    Serve one or more Catalogs.

    Parameters
    ----------
    catalogs : Dict[Tuple[str, ...], Catalog]
        Each key should be a tuple representing a sub-path, i.e. ('a', 'b').
        Each value should be a Catalog.
    authenticator : Authenticator

    Examples
    --------

    Serve one Catalog and the root path /.

    >>> serve_catalogs({(): catalog})

    Serve two Catalogs under /a and /b/c respectively.
    >>> serve_catalogs({('a'): catalog1, ('b', 'c'): catalog2})

    See Also
    --------
    serve_catalog
    """

    @lru_cache(1)
    def override_get_authenticator():
        return authenticator

    @lru_cache(1)
    def override_get_root_catalog():
        mapping = {}
        for segments, catalog in catalogs.items():
            if not segments:
                # There is one root_catalog to be deployed at '/'.
                return catalog
            inner_mapping = mapping
            for segment in segments[:-1]:
                if segment in inner_mapping:
                    inner_mapping = inner_mapping[segment]
                else:
                    inner_mapping = inner_mapping[segment] = {}
            inner_mapping[segments[-1]] = catalog
        return Catalog(mapping)

    # The Catalog and Authenticator have the opporunity to add custom routes to
    # the server here. (Just for example, a Catalog of BlueskyRuns uses this
    # hook to add a /documents route.) This has to be done before dependency_overrides
    # are processed, so we cannot just inject this configuration via Depends.
    include_routers = []
    # TODO Give some thought to the way that we merge routes from different
    # Catalogs here. I don't see any show-stopping problems with this but it
    # feels a bit weird.
    for catalog in catalogs.values():
        include_routers.extend(getattr(catalog, "include_routers", []))
    include_routers.extend(getattr(authenticator, "include_routers", []))
    app = get_app(include_routers=include_routers)
    app.dependency_overrides[get_authenticator] = override_get_authenticator
    app.dependency_overrides[get_root_catalog] = override_get_root_catalog
    return app


def serve_catalog(catalog, authenticator=None):
    """
    Serve a Catalogs.

    Parameters
    ----------
    catalog : Catalog
    authenticator : Authenticator

    See Also
    --------
    serve_catalogs
    """
    return serve_catalogs({(): catalog}, authenticator=authenticator)
