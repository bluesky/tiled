import contextlib
import getpass

import pytest

from ..client import context
from ..server.settings import get_settings


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
def set_auth_token_cache_dir(tmpdir):
    """
    Use a tmpdir instead of ~/.cache/tiled/tokens
    """
    original = context.DEFAULT_TOKEN_CACHE
    context.DEFAULT_TOKEN_CACHE = str(tmpdir)
    yield
    context.DEFAULT_TOKEN_CACHE = original


@pytest.fixture
def enter_password(monkeypatch):
    """
    Return a context manager that overrides getpass, used like:

    >>> with enter_password(...):
    ...     # Run code that calls getpass.getpass().
    """

    @contextlib.contextmanager
    def f(password):
        context.PROMPT_FOR_REAUTHENTICATION = True
        original = getpass.getpass
        monkeypatch.setattr("getpass.getpass", lambda: password)
        yield
        monkeypatch.setattr("getpass.getpass", original)
        context.PROMPT_FOR_REAUTHENTICATION = None

    return f


@pytest.fixture(scope="module")
def tmpdir_module(request, tmpdir_factory):
    """A tmpdir fixture for the module scope. Persists throughout the module."""
    # Source: https://stackoverflow.com/a/31889843
    return tmpdir_factory.mktemp(request.module.__name__)


# This can un-commented to debug leaked threads.
# import threading
# import time
#
# def poll_enumerate():
#     while True:
#         time.sleep(1)
#         print("THREAD COUNT", len(threading.enumerate()))
#
# thread = threading.Thread(target=poll_enumerate, daemon=True)
# thread.start()
