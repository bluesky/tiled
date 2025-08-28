import io
import os
import sys
import tempfile
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
import stamina
from sqlalchemy.ext.asyncio import create_async_engine

from .. import profiles
from ..catalog import from_uri, in_memory
from ..client.base import BaseClient
from ..server.settings import get_settings
from ..utils import ensure_specified_sql_driver
from .utils import enter_username_password as utils_enter_uname_passwd
from .utils import temp_postgres


@pytest.fixture(autouse=True, scope="session")
def deactivate_retries():
    "Deactivate HTTP retries."
    stamina.set_active(False)


@pytest.fixture(autouse=True)
def reset_settings():
    """
    Reset the FastAPI Settings.

    Fast API uses a global singleton for Settings.  It is difficult to get
    around this, so our best option is to reset it.
    """
    get_settings.cache_clear()
    yield


@pytest.fixture(autouse=True)
def set_tiled_cache_dir():
    """
    Use a tmpdir instead of ~/.cache/tiled/tokens
    """
    # Do not use tempdir pytest fixture because it would use the same tmpdir
    # as the one used by the test, and mix the files up.
    # Windows will not remove the directory while the http_response_cache.db
    # is still open. It is closed by transport shutdown, but not all tests
    # correctly shut down the transport. This is probably related to the
    # thread-leaking issue.
    # This option was added to TemporaryDirectory in Python 3.10
    kwargs = {}
    if sys.platform.startswith("win") and sys.version_info >= (3, 10):
        kwargs["ignore_cleanup_errors"] = True
    with tempfile.TemporaryDirectory(**kwargs) as tmpdir:
        os.environ["TILED_CACHE_DIR"] = str(tmpdir)
        yield
        del os.environ["TILED_CACHE_DIR"]


@pytest.fixture(scope="function")
def buffer():
    "Generate a temporary buffer for testing file export + re-import."
    with io.BytesIO() as buffer:
        yield buffer


@pytest.fixture(scope="function")
def buffer_factory():
    buffers = []

    def _buffer():
        buf = io.BytesIO()
        buffers.append(buf)
        return buf

    yield _buffer

    for buf in buffers:
        buf.close()


@pytest.fixture
def tmp_profiles_dir():
    """
    Use a tmpdir instead of ~/.config/tiled/profiles
    """
    # Do not use tempdir pytest fixture because it would use the same tmpdir
    # as the one used by the test, and mix the files up.
    with tempfile.TemporaryDirectory() as tmpdir:
        original = profiles.paths
        profiles.paths.clear()
        profiles.paths.extend([Path(tmpdir)])
        profiles.load_profiles.cache_clear()
        yield
        profiles.paths.clear()
        profiles.paths.extend(original)


@pytest.fixture
def enter_username_password():
    """
    DEPRECATED: Use the normal (non-fixture) context manager in .utils.
    """
    return utils_enter_uname_passwd


@pytest.fixture(scope="module")
def tmpdir_module(request, tmpdir_factory):
    """A tmpdir fixture for the module scope. Persists throughout the module."""
    # Source: https://stackoverflow.com/a/31889843
    return tmpdir_factory.mktemp(request.module.__name__)


# Use this with pytest --log-cli-level=25 option.
if os.getenv("TILED_DEBUG_LEAKED_THREADS"):
    import logging
    import threading
    import time

    def poll_enumerate():
        logger = logging.getLogger(__name__)
        msg_level = int(logging.INFO + logging.WARNING) // 2
        while True:
            time.sleep(1)
            logger.log(msg_level, "THREAD COUNT = %d", len(threading.enumerate()))

    thread = threading.Thread(target=poll_enumerate, daemon=True)
    thread.start()


# To test with postgres, start a container like:
#
# docker run --name tiled-test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -d docker.io/postgres:16
# and set this env var like:
#
# TILED_TEST_POSTGRESQL_URI=postgresql+asyncpg://postgres:secret@localhost:5432

TILED_TEST_POSTGRESQL_URI = os.getenv("TILED_TEST_POSTGRESQL_URI")


@pytest_asyncio.fixture
async def sqlite_uri(tmp_path: Path):
    yield f"sqlite:///{tmp_path}/tiled.sqlite"


@pytest_asyncio.fixture(scope="function")
async def duckdb_uri(tmp_path: Path):
    yield f"duckdb:///{tmp_path}/tiled.duckdb"


@pytest_asyncio.fixture
async def postgres_uri():
    if not TILED_TEST_POSTGRESQL_URI:
        raise pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
    async with temp_postgres(TILED_TEST_POSTGRESQL_URI) as uri_with_database:
        yield uri_with_database


@pytest.fixture(params=["sqlite_uri", "postgres_uri"])
def sqlite_or_postgres_uri(request):
    yield request.getfixturevalue(request.param)


@pytest.fixture(params=["sqlite_uri", "duckdb_uri", "postgres_uri"])
def sql_storage_uri(request):
    yield request.getfixturevalue(request.param)


@pytest_asyncio.fixture
async def postgresql_adapter(request, tmpdir):
    """
    Adapter instance

    Note that startup() and shutdown() are not called, and must be run
    either manually (as in the fixture 'a') or via the app (as in the fixture 'client').
    """
    if not TILED_TEST_POSTGRESQL_URI:
        raise pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
    # Create temporary database.
    async with temp_postgres(TILED_TEST_POSTGRESQL_URI) as uri_with_database_name:
        # Build an adapter on it, and initialize the database.
        adapter = from_uri(
            uri_with_database_name,
            writable_storage=str(tmpdir),
            init_if_not_exists=True,
        )
        yield adapter


@pytest_asyncio.fixture
async def postgresql_with_example_data_adapter(request, tmpdir):
    """
    Adapter instance

    Note that startup() and shutdown() are not called, and must be run
    either manually (as in the fixture 'a') or via the app (as in the fixture 'client').
    """
    if not TILED_TEST_POSTGRESQL_URI:
        raise pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
    DATABASE_NAME = "tiled-example-data"
    uri = TILED_TEST_POSTGRESQL_URI
    if uri.endswith("/"):
        uri = uri[:-1]
    uri_with_database_name = f"{uri}/{DATABASE_NAME}"
    engine = create_async_engine(ensure_specified_sql_driver(uri_with_database_name))
    try:
        async with engine.connect():
            pass
    except asyncpg.exceptions.InvalidCatalogNameError:
        raise pytest.skip(
            f"PostgreSQL instance contains no database named {DATABASE_NAME!r}"
        )
    adapter = from_uri(
        uri_with_database_name,
        writable_storage=str(tmpdir),
    )
    yield adapter


@pytest_asyncio.fixture
async def sqlite_adapter(request, tmpdir):
    """
    Adapter instance

    Note that startup() and shutdown() are not called, and must be run
    either manually (as in the fixture 'a') or via the app (as in the fixture 'client').
    """
    yield in_memory(writable_storage=str(tmpdir))


@pytest_asyncio.fixture
async def sqlite_with_example_data_adapter(request, tmpdir):
    """
    Adapter instance

    Note that startup() and shutdown() are not called, and must be run
    either manually (as in the fixture 'a') or via the app (as in the fixture 'client').
    """
    SQLITE_DATABASE_PATH = Path("./tiled_test_db_sqlite.db")
    if not SQLITE_DATABASE_PATH.is_file():
        raise pytest.skip(f"Could not find {SQLITE_DATABASE_PATH}")
    adapter = from_uri(
        f"sqlite:///{SQLITE_DATABASE_PATH}",
        writable_storage=str(tmpdir),
    )
    yield adapter


@pytest.fixture(params=["sqlite_adapter", "postgresql_adapter"])
def adapter(request):
    """
    Adapter instance

    Note that startup() and shutdown() are not called, and must be run
    either manually (as in the fixture 'a') or via the app (as in the fixture 'client').
    """
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    params=["sqlite_with_example_data_adapter", "postgresql_with_example_data_adapter"]
)
def example_data_adapter(request):
    """
    Adapter instance

    Note that startup() and shutdown() are not called, and must be run
    either manually (as in the fixture 'a') or via the app (as in the fixture 'client').
    """
    yield request.getfixturevalue(request.param)


@pytest.fixture(scope="module")
def set_and_deplete(request: pytest.FixtureRequest):
    "Initial values must all be removed or else calling tests fail"
    values = set(request.param)
    yield values
    assert not values


@pytest.fixture
def url_limit(request: pytest.FixtureRequest):
    """Adjust the URL length limit for the client's GET requests.

    Data will be fetched by GET requests when the URL_CHARACTER_LIMIT is long,
    and by equivalent POST requests when the URL_CHARACTER_LIMIT is short.
    """
    URL_CHARACTER_LIMIT = int(request.param)
    PREVIOUS_LIMIT = BaseClient.URL_CHARACTER_LIMIT
    # Temporarily adjust the URL length limit to change client behavior.
    BaseClient.URL_CHARACTER_LIMIT = URL_CHARACTER_LIMIT
    yield
    # Then restore the original value.
    BaseClient.URL_CHARACTER_LIMIT = PREVIOUS_LIMIT


@pytest.fixture
def redis_uri():
    if uri := os.getenv("TILED_TEST_REDIS"):
        import redis

        client = redis.from_url(uri, socket_timeout=10, socket_connect_timeout=30)
        # Delete all keys from the current database before and after test.
        client.flushdb()
        yield uri
        client.flushdb()
    else:
        raise pytest.skip("No TILED_TEST_REDIS configured")
