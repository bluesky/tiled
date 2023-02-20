import contextlib

import httpx
import pytest


def force_update(client):
    """
    Reach into the tree force it to process an updates. Block until complete.

    We could just wait (i.e. sleep) instead until the next polling loop completes,
    but this makes the tests take longer to run, and it can be flaky on CI services
    where things can take longer than they do in normal use.
    """
    client.context.http_client.app.state.root_tree.update_now()


@contextlib.contextmanager
def fail_with_status_code(status_code):
    with pytest.raises(httpx.HTTPStatusError) as info:
        yield
    assert info.value.response.status_code == status_code
