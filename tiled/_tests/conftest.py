import os
import sys
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from .. import profiles
from ..catalog import from_uri, in_memory
from ..server.settings import get_settings
from .utils import enter_password as utils_enter_password
from .utils import temp_postgres


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
def enter_password():
    """
    DEPRECATED: Use the normal (non-fixture) context manager in .utils.
    """
    return utils_enter_password


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
# docker run --name tiled-test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -d docker.io/postgres
# and set this env var like:
#
# TILED_TEST_POSTGRESQL_URI=postgresql+asyncpg://postgres:secret@localhost:5432

TILED_TEST_POSTGRESQL_URI = os.getenv("TILED_TEST_POSTGRESQL_URI")


@pytest_asyncio.fixture(params=["sqlite", "postgresql"])
async def adapter(request, tmpdir):
    """
    Adapter instance

    Note that startup() and shutdown() are not called, and must be run
    either manually (as in the fixture 'a') or via the app (as in the fixture 'client').
    """
    if request.param == "sqlite":
        adapter = in_memory(writable_storage=str(tmpdir))
        yield adapter
    elif request.param == "postgresql":
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
            await adapter.shutdown()
    else:
        assert False
