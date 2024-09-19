import contextlib
import threading
import time

import pytest
import uvicorn
from fastapi import APIRouter
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from ..catalog import in_memory
from ..client import from_uri
from ..server.app import build_app
from ..server.logging_config import LOGGING_CONFIG

router = APIRouter()


class Server(uvicorn.Server):
    # https://github.com/encode/uvicorn/discussions/1103#discussioncomment-941726

    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        thread = threading.Thread(target=self.run)
        thread.start()
        try:
            # Wait for server to start up, or raise TimeoutError.
            for _ in range(100):
                time.sleep(0.1)
                if self.started:
                    break
            else:
                raise TimeoutError("Server did not start in 10 seconds.")
            host, port = self.servers[0].sockets[0].getsockname()
            yield f"http://{host}:{port}"
        finally:
            self.should_exit = True
            thread.join()


API_KEY = "secret"


@pytest.fixture
def server(tmpdir):
    catalog = in_memory(writable_storage=tmpdir)
    app = build_app(catalog, {"single_user_api_key": API_KEY})
    app.include_router(router)
    config = uvicorn.Config(app, port=0, loop="asyncio", log_config=LOGGING_CONFIG)
    server = Server(config)
    with server.run_in_thread() as url:
        yield url


@router.get("/error")
def error():
    1 / 0  # error!


def test_500_response(server):
    """
    Test that unexpected server error returns 500 response.

    This test is meant to catch regressions in which server exceptions can
    result in the server sending no response at all, leading clients to raise
    like:

    httpx.RemoteProtocolError: Server disconnected without sending a response.

    This can happen when bugs are introduced in the middleware layer.
    """
    client = from_uri(server, api_key=API_KEY)
    response = client.context.http_client.get(f"{server}/error")
    assert response.status_code == HTTP_500_INTERNAL_SERVER_ERROR


def test_writing_integration(server):
    client = from_uri(server, api_key=API_KEY)
    x = client.write_array([1, 2, 3], key="array")
    x[:]
