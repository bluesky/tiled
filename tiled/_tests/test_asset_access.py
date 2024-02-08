import hashlib
from pathlib import Path

import pytest

from ..catalog import in_memory
from ..client import Context, from_context
from ..client.utils import get_asset_filepaths
from ..server.app import build_app
from ..utils import path_from_uri


@pytest.fixture
def context(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        yield context


@pytest.fixture
def client(context):
    client = from_context(context)
    yield client


def test_include_data_sources_method_on_self(client):
    "Calling include_data_sources() fetches data sources on self."
    client.write_array([1, 2, 3], key="x")
    with pytest.warns(UserWarning):
        # This fetches the sources with an additional implicit request.
        client["x"].data_sources()
    client["x"].include_data_sources().data_sources() is not None


def test_include_data_sources_method_affects_children(client):
    "Calling include_data_sources() fetches data sources on children."
    client.create_container("c")
    client["c"].write_array([1, 2, 3], key="x")
    c = client["c"].include_data_sources()
    c["x"].data_sources() is not None


def test_include_data_sources_kwarg(context):
    "Passing include_data_sources to constructor includes them by default."
    client = from_context(context, include_data_sources=True)
    client.write_array([1, 2, 3], key="x")
    client["x"].data_sources() is not None
    client["x"].include_data_sources() is not None


def test_raw_export(client, tmpdir):
    "Use raw_export() and compare hashes or original and exported files."
    client.write_array([1, 2, 3], key="x")
    exported_paths = client["x"].raw_export(tmpdir)
    data_sources = client["x"].include_data_sources().data_sources()
    orig_dir = path_from_uri(data_sources[0]["assets"][0]["data_uri"])
    _asset_id, relative_paths = client["x"].asset_manifest(data_sources).popitem()
    orig_paths = [Path(orig_dir, relative_path) for relative_path in relative_paths]
    orig_hashes = [hashlib.md5(path.read_bytes()).digest() for path in orig_paths]
    exported_hashes = [
        hashlib.md5(path.read_bytes()).digest() for path in exported_paths
    ]
    assert orig_hashes == exported_hashes


def test_get_asset_filepaths(client):
    client.write_array([1, 2, 3], key="x")
    get_asset_filepaths(client.include_data_sources()["x"])
