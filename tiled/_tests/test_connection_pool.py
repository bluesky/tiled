from typing import Union

import pytest
from sqlalchemy.engine import URL, make_url

from tiled.server.connection_pool import is_memory_sqlite


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("sqlite://", True),  # accepts str
        (make_url("sqlite://"), True),  # accepts URL
        ("sqlite:///:memory:", True),
        ("sqlite:///file::memory:?cache=shared", True),
        ("sqlite:///file:name:?cache=shared&mode=memory", True),
        ("sqlite:////tmp/example.db", False),
    ],
)
def test_is_memory_sqlite(uri: Union[str, URL], expected: bool):
    actual = is_memory_sqlite(uri)
    assert actual is expected
