import asyncio
import logging

import httpx
import numpy
import pytest
import uvicorn
from fastapi import APIRouter
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.catalog import in_memory
from tiled.client import from_uri
from tiled.config import Authentication, Database
from tiled.server.app import build_app, build_app_from_config
from tiled.server.logging_config import LOGGING_CONFIG

from .conftest import TOY_AUTHENTICATION, init_auth_database
from .utils import Server

router = APIRouter()

API_KEY = "secret"


@pytest.fixture
def server(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog, Authentication(single_user_api_key=API_KEY))
    app.include_router(router)
    config = uvicorn.Config(app, port=0, loop="asyncio", log_config=LOGGING_CONFIG)
    server = Server(config)
    with server.run_in_thread() as url:
        yield url


@pytest.fixture
def public_server(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(
        catalog,
        Authentication(single_user_api_key=API_KEY, allow_anonymous_access=True),
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
    database_uri = init_auth_database(tmpdir)
    config = {
        "authentication": TOY_AUTHENTICATION,
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


@pytest.mark.parametrize("root_path", ["", "/tiled"])
def test_about_reports_api_root_path(tmpdir, root_path):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog, Authentication(single_user_api_key=API_KEY))
    # uvicorn prepends root_path to the request path, as behind a proxy.
    config = uvicorn.Config(
        app, port=0, loop="asyncio", log_config=LOGGING_CONFIG, root_path=root_path
    )
    with Server(config).run_in_thread() as url:
        response = httpx.get(url + "/api/v1/")

    assert response.json()["meta"]["root_path"] == f"{root_path}/api"


@pytest.mark.asyncio
async def test_auth_database_purge_task_logs_and_retries_after_failure(
    monkeypatch, caplog
):
    attempts = 0
    retried = asyncio.Event()
    release_purge = asyncio.Event()
    purge_task = None

    async def fail_then_block(db_session, model):
        nonlocal attempts, purge_task
        attempts += 1
        purge_task = asyncio.current_task()
        if attempts == 1:
            raise RuntimeError("database unavailable")
        retried.set()
        await release_purge.wait()

    async def no_wait(delay):
        pass

    monkeypatch.setattr("tiled.authn_database.core.purge_expired", fail_then_block)
    monkeypatch.setattr("tiled.server.app.asyncio.sleep", no_wait)

    app = build_app(
        MapAdapter({}),
        server_settings={"database": Database(uri="sqlite:///:memory:")},
    )
    try:
        with caplog.at_level(logging.WARNING, logger="tiled.server.app"):
            async with app.router.lifespan_context(app):
                await asyncio.wait_for(retried.wait(), timeout=1)
                assert purge_task is not None
                assert purge_task in app.state.tasks
                assert not purge_task.done()
    finally:
        if purge_task is not None:
            done, pending = await asyncio.wait({purge_task}, timeout=1)
            assert not pending, "Purge task did not stop during lifespan shutdown."
            await asyncio.gather(*done, return_exceptions=True)

    warning = next(
        record
        for record in caplog.records
        if record.getMessage()
        == "Failed to purge expired Sessions and API keys from the database."
    )
    assert warning.levelno == logging.WARNING
    assert warning.exc_info is not None
    assert isinstance(warning.exc_info[1], RuntimeError)
    assert str(warning.exc_info[1]) == "database unavailable"
