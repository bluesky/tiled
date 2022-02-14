import contextlib
import getpass

import pytest

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


@pytest.fixture
def enter_password(monkeypatch):
    """
    Return a context manager that overrides getpass, used like:

    >>> with enter_password(...):
    ...     # Run code that calls getpass.getpass().
    """

    @contextlib.contextmanager
    def f(password):
        original = getpass.getpass
        monkeypatch.setattr("getpass.getpass", lambda: password)
        yield
        monkeypatch.setattr("getpass.getpass", original)

    return f
