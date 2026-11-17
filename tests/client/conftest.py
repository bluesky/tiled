import pytest
import pytest_asyncio

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.client.aio import from_app as aio_from_app
from tiled.server.app import build_app
from tiled.utils import ensure_uri


@pytest.fixture(scope="module")
def app(tmp_path_factory):
    """An in-process Tiled app with writable file and SQL (appendable-table) storage.

    The app owns stateful async machinery (the catalog's aiosqlite connection
    and its locks) that must be used from one event loop only. It is shared
    per module for speed, so every async test and fixture in the consuming
    modules must run on the module-scoped event loop (loop_scope="module");
    with per-test loops, tests fail intermittently with "... is bound to a
    different event loop" once two tests contend the same lock.
    """
    tmp_path = tmp_path_factory.mktemp("client_app")
    catalog = in_memory(
        writable_storage=[
            ensure_uri(tmp_path / "data"),
            f"duckdb:///{tmp_path / 'data.duckdb'}",
        ]
    )
    return build_app(catalog)


@pytest_asyncio.fixture(loop_scope="module")
async def aclient(app):
    "An asynchronous client connected to `app` (driving the app's lifespan)."
    client = await aio_from_app(app)
    async with client.context:
        yield client


@pytest.fixture
def sync_client(app):
    # The app's lifespan is already running, driven by the async context.
    with Context.from_app(app) as context:
        yield from_context(context)
