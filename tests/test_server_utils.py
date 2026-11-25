import asyncio

import numpy
import pytest
from starlette.requests import Request

from tiled.access_control.access_policies import DummyAccessPolicy
from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.client import Context, from_context
from tiled.queries import Key
from tiled.server.app import build_app
from tiled.server.utils import get_current_url, get_root_url, normalize_root_path


def make_request(path: str, root_path: str, headers: dict | None = None) -> Request:
    headers = headers or {"host": "example.org"}
    return Request(
        {
            "type": "http",
            "scheme": "http",
            "path": path,
            "root_path": root_path,
            "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
        }
    )


@pytest.mark.parametrize(
    "root_path,expected",
    [
        (None, ""),
        ("", ""),
        ("/", ""),
        ("//", ""),
        ("/tiled", "/tiled"),
        ("/tiled/", "/tiled"),
        ("tiled", "/tiled"),
        ("/a/b/", "/a/b"),
        ("/tenant/ui/tiled", "/tenant/ui/tiled"),
    ],
)
def test_normalize_root_path(root_path: str | None, expected: str):
    assert normalize_root_path(root_path) == expected


@pytest.mark.parametrize(
    "path,root_path,expected",
    [
        (
            # The ASGI server prepends root_path to path, so root_path must be
            # counted exactly once.
            "/tiled/api/v1/auth/provider/orcid/code",
            "/tiled",
            "http://example.org/tiled/api/v1/auth/provider/orcid/code",
        ),
        (
            "/api/v1/auth/provider/orcid/code",
            "",
            "http://example.org/api/v1/auth/provider/orcid/code",
        ),
    ],
)
def test_get_current_url(path: str, root_path: str, expected: str):
    assert get_current_url(make_request(path, root_path=root_path)) == expected


def test_get_root_url_honors_forwarded_headers():
    request = make_request(
        "/tiled/api/v1",
        root_path="/tiled",
        headers={
            "host": "localhost:8000",
            "x-forwarded-host": "example.com",
            "x-forwarded-proto": "https",
        },
    )
    assert get_root_url(request) == "https://example.com/tiled"


class _SearchRecordingMapAdapter(MapAdapter):
    "Record whether ``search`` ran on the server's event loop thread."

    ran_on_event_loop = []

    def search(self, query):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self.ran_on_event_loop.append(False)
        else:
            self.ran_on_event_loop.append(True)
        return super().search(query)


class _DogsOnlyAccessPolicy(DummyAccessPolicy):
    async def filters(self, node, principal, authn_access_tags, authn_scopes, scopes):
        return [Key("animal") == "dog"]


def test_access_filters_applied_to_sync_container_off_the_event_loop():
    """
    The queries of an access policy are applied with ``search``. A container
    with the synchronous interface may evaluate the query there (MapAdapter
    walks its entries), or block on I/O. The server must not do that on the
    event loop.
    """
    tree = _SearchRecordingMapAdapter(
        {
            "a": ArrayAdapter.from_array(numpy.arange(3), metadata={"animal": "dog"}),
            "b": ArrayAdapter.from_array(numpy.arange(3), metadata={"animal": "cat"}),
        }
    )
    record = _SearchRecordingMapAdapter.ran_on_event_loop
    app = build_app(tree, access_policy=_DogsOnlyAccessPolicy())
    with Context.from_app(app) as context:
        client = from_context(context)
        record.clear()
        assert list(client) == ["a"]
    assert record  # It was called...
    assert not any(record)  # ...in a thread.
