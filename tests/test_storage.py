"""Tests for `tiled.storage`."""

import pytest

from tiled.storage import size_from_uri


def test_size_from_uri_file(tmp_path):
    """`size_from_uri` returns the byte length of a local file:// URI."""
    p = tmp_path / "blob.bin"
    payload = b"the quick brown fox"
    p.write_bytes(payload)
    assert size_from_uri(p.as_uri()) == len(payload)


def test_size_from_uri_unsupported_scheme():
    """`size_from_uri` rejects schemes outside `file` and the supported
    object-store set with a clear `ValueError`."""
    with pytest.raises(ValueError, match="unsupported scheme"):
        size_from_uri("ftp://example.com/blob.bin")


def test_size_from_uri_missing_file(tmp_path):
    """Underlying I/O errors propagate; callers wanting best-effort behavior
    must catch them at the call site."""
    missing = tmp_path / "does-not-exist.bin"
    with pytest.raises(FileNotFoundError):
        size_from_uri(missing.as_uri())
