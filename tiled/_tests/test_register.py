from pathlib import Path

from ..catalog.register import resolve_mimetype


def test_resolve_mimetype():
    assert resolve_mimetype(Path("test.csv")) == "text/csv"
