from typing import Optional

import pytest
from starlette.requests import Request

from tiled.server.utils import get_root_url, normalize_root_path


def make_request(path: str, root_path: str, headers: Optional[dict] = None) -> Request:
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
def test_normalize_root_path(root_path: Optional[str], expected: str):
    assert normalize_root_path(root_path) == expected


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
