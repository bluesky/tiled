from pathlib import Path

from ..client.register import DEFAULT_MIMETYPES_BY_FILE_EXT, resolve_mimetype


def test_resolve_mimetype() -> None:
    assert (
        resolve_mimetype(Path("test.csv"), DEFAULT_MIMETYPES_BY_FILE_EXT) == "text/csv"
    )
