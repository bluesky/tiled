import subprocess
import sys

import httpx
import numpy
import pytest
import uvicorn
from fastapi import APIRouter
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..catalog import in_memory
from ..client import from_uri
from ..server.app import build_app, build_app_from_config
from ..server.logging_config import LOGGING_CONFIG
from .utils import Server

router = APIRouter()

API_KEY = "secret"


@pytest.fixture
def server(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog, {"single_user_api_key": API_KEY})
    app.include_router(router)
    config = uvicorn.Config(app, port=0, loop="asyncio", log_config=LOGGING_CONFIG)
    server = Server(config)
    with server.run_in_thread() as url:
        yield url


@pytest.fixture
def public_server(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(
        catalog, {"single_user_api_key": API_KEY, "allow_anonymous_access": True}
    )
    app.include_router(router)
    config = uvicorn.Config(app, port=0, loop="asyncio", log_config=LOGGING_CONFIG)
    server = Server(config)
    with server.run_in_thread() as url:
        yield url


arr = ArrayAdapter.from_array(numpy.ones((5, 5)))
tree = MapAdapter({"A1": arr, "A2": arr})


@pytest.fixture
def multiuser_server(tmpdir):
    database_uri = f"sqlite:///{tmpdir}/tiled.sqlite"
    subprocess.run(
        [sys.executable, "-m", "tiled", "admin", "initialize-database", database_uri],
        check=True,
        capture_output=True,
    )
    config = {
        "authentication": {
            "secret_keys": ["SECRET"],
            "providers": [
                {
                    "provider": "toy",
                    "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                    "args": {
                        "users_to_passwords": {"alice": "secret1", "bob": "secret2"}
                    },
                }
            ],
        },
        "database": {
            "uri": database_uri,
        },
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/",
            },
        ],
    }
    app = build_app_from_config(config)
    app.include_router(router)
    config = uvicorn.Config(app, port=0, loop="asyncio", log_config=LOGGING_CONFIG)
    server = Server(config)
    with server.run_in_thread() as url:
        yield url


@router.get("/error")
def error():
    1 / 0  # type: ignore error!


@pytest.mark.filterwarnings("ignore: websockets.legacy is deprecated")
@pytest.mark.filterwarnings(
    "ignore: websockets.server.WebSocketServerProtocol is deprecated"
)
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


def test_public_server(public_server):
    from_uri(public_server)


def test_internal_authentication_mode_with_password_clients(multiuser_server):
    "The 'internal' authentication mode used to be named 'password'."
    # Mock old client
    response = httpx.get(
        multiuser_server + "/api/v1/", headers={"user-agent": "python-tiled/0.1.0b16"}
    )
    actual_mode = response.json()["authentication"]["providers"][0]["mode"]
    assert actual_mode == "password"

    # Mock new client
    response = httpx.get(
        multiuser_server + "/api/v1/", headers={"user-agent": "python-tiled/0.1.0b17"}
    )
    actual_mode = response.json()["authentication"]["providers"][0]["mode"]
    assert actual_mode == "internal"

    # Mock unknown client
    response = httpx.get(multiuser_server + "/api/v1/", headers={})
    actual_mode = response.json()["authentication"]["providers"][0]["mode"]
    assert actual_mode == "internal"
