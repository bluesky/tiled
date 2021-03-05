import os

from fastapi import FastAPI
from msgpack_asgi import MessagePackMiddleware

from .router import declare_search_route, router
from .utils import get_settings


api = FastAPI()


@api.on_event("startup")
async def startup_event():
    # The /search route is defined as server startup so that the user has the
    # opporunity to register custom query types before startup.
    declare_search_route(router)
    api.include_router(router)
    # Warm up cached access.
    get_settings().catalog
    # get_dask_client()


@api.on_event("shutdown")
async def shutdown_event():
    # client = get_dask_client()
    # await client.close()
    pass


if not os.getenv("DISABLE_MSGPACK_MIDDLEWARE"):
    app = MessagePackMiddleware(api)
else:
    app = api


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
