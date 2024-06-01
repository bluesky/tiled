import asyncio
import functools

import anyio
import uvicorn
from fastapi import APIRouter
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from ..catalog import in_memory
from ..client import from_uri
from ..server.app import build_app
from ..server.logging_config import LOGGING_CONFIG

router = APIRouter()


@router.get("/error")
def error():
    1 / 0  # error!


def test_500_response():
    """
    Test that unexpected server error returns 500 response.

    This test is meant to catch regressions in which server exceptions can
    result in the server sending no response at all, leading clients to raise
    like:

    httpx.RemoteProtocolError: Server disconnected without sending a response.

    This can happen when bugs are introduced in the middleware layer.
    """
    API_KEY = "secret"
    catalog = in_memory()
    app = build_app(catalog, {"single_user_api_key": API_KEY})
    app.include_router(router)
    config = uvicorn.Config(app, port=0, log_config=LOGGING_CONFIG)
    server = uvicorn.Server(config)

    async def run_server():
        print("run_server")
        await server.serve()

    async def wait_for_server():
        "Wait for server to start up, or raise TimeoutError."
        for _ in range(100):
            await asyncio.sleep(0.1)
            if server.started:
                break
        else:
            raise TimeoutError("Server did not start in 10 seconds.")
        host, port = server.servers[0].sockets[0].getsockname()
        return f"http://{host}:{port}"

    async def test():
        server_task = asyncio.create_task(run_server())
        url = await wait_for_server()

        # When we add an AsyncClient for Tiled, use that here.
        client = await anyio.to_thread.run_sync(
            functools.partial(from_uri, url, api_key=API_KEY)
        )
        response = await anyio.to_thread.run_sync(
            client.context.http_client.get, f"{url}/error"
        )
        assert response.status_code == HTTP_500_INTERNAL_SERVER_ERROR
        print("asserted")
        await server_task.shutdown()
        await server_task()

    asyncio.run(test())
