"""Tests for `tiled.storage`."""

import pytest

from tiled.storage import size_from_uri, stat_uri


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


def test_stat_uri_file(tmp_path):
    """`stat_uri` reports a regular file as not-a-directory with its size."""
    p = tmp_path / "blob.bin"
    payload = b"the quick brown fox"
    p.write_bytes(payload)
    result = stat_uri(p.as_uri())
    assert result == (False, len(payload))
    # Fields are also accessible by name.
    assert result.is_directory is False
    assert result.size == len(payload)


def test_stat_uri_directory(tmp_path):
    """`stat_uri` reports a directory as such, with no size."""
    d = tmp_path / "store.zarr"
    d.mkdir()
    assert stat_uri(d.as_uri()) == (True, None)


def test_stat_uri_missing_file_is_best_effort(tmp_path):
    """Unlike `size_from_uri`, `stat_uri` swallows I/O errors and returns
    `(False, None)` since the fields are advisory."""
    missing = tmp_path / "does-not-exist.bin"
    assert stat_uri(missing.as_uri()) == (False, None)


def test_stat_uri_unsupported_scheme_is_best_effort():
    """An un-inspectable scheme yields `(False, None)` rather than raising."""
    assert stat_uri("ftp://example.com/blob.bin") == (False, None)
