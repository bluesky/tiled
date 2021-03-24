import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from .settings import get_settings, get_custom_routers
from .router import declare_search_route, router
from .core import PatchedStreamingResponse


api = FastAPI()


@api.on_event("startup")
async def startup_event():
    # The /search route is defined at server startup so that the user has the
    # opporunity to register custom query types before startup.
    declare_search_route(router)
    api.include_router(router)
    for custom_router in get_custom_routers():
        api.include_router(custom_router)
    # Warm up cached access.
    get_settings().catalog
    # get_dask_client()


@api.on_event("shutdown")
async def shutdown_event():
    # client = get_dask_client()
    # await client.close()
    pass


@api.middleware("http")
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


api.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(api, host="0.0.0.0", port=8000)
