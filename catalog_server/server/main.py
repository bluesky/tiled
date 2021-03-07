from fastapi import FastAPI

from .settings import get_settings, get_custom_routers
from .router import declare_search_route, router


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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(api, host="0.0.0.0", port=8000)
