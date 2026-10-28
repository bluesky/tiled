import hashlib
from pathlib import Path

import pandas
import pytest
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
    HTTP_416_RANGE_NOT_SATISFIABLE,
)

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.client.utils import get_asset_filepaths
from tiled.server.app import build_app
from tiled.utils import path_from_uri

from .utils import fail_with_status_code


@pytest.fixture
def context(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir / "data"))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        yield context


@pytest.fixture
def client(context):
    client = from_context(context)
    yield client


def test_include_data_sources_method_on_self(client):
    "Calling include_data_sources() fetches data sources on self."
    x = client.write_array([1, 2, 3], key="x")
    # Fetch data_sources on x object directly.
    assert x.data_sources() is not None
    # Fetch data_sources on x object, looked up in client.
    assert client["x"].data_sources() is not None
    assert client["x"].include_data_sources().data_sources() is not None


def test_include_data_sources_method_affects_children(client):
    "Calling include_data_sources() fetches data sources on children."
    client.create_container("c")
    client["c"].write_array([1, 2, 3], key="x")
    c = client["c"].include_data_sources()
    assert c["x"].data_sources() is not None


def test_include_data_sources_kwarg(context):
    "Passing include_data_sources to constructor includes them by default."
    client = from_context(context, include_data_sources=True)
    client.write_array([1, 2, 3], key="x")
    assert client["x"].data_sources() is not None
    assert client["x"].include_data_sources() is not None


def test_raw_export(client, tmpdir):
    "Use raw_export() and compare hashes or original and exported files."
    client.write_array([1, 2, 3], key="x")
    exported_paths = client["x"].raw_export(tmpdir / "exported")
    data_sources = client["x"].include_data_sources().data_sources()
    orig_dir = path_from_uri(data_sources[0].assets[0].data_uri)
    _asset_id, relative_paths = client["x"].asset_manifest(data_sources).popitem()
    orig_paths = [Path(orig_dir, relative_path) for relative_path in relative_paths]
    orig_hashes = [hashlib.md5(path.read_bytes()).digest() for path in orig_paths]
    exported_hashes = [
        hashlib.md5(path.read_bytes()).digest() for path in exported_paths
    ]
    assert orig_hashes == exported_hashes


def test_asset_range_request(client, tmpdir):
    "Access part of an asset using an HTTP Range header."
    df = pandas.DataFrame({"A": [1, 2, 3], "B": [4.0, 5.0, 6.0]})
    client.write_table(df, key="x")
    # Fetch the first byte.
    first_byte_response = client.context.http_client.get(
        "/api/v1/asset/bytes/x?id=1",
        headers={"Range": "bytes=0-0"},
    )
    assert first_byte_response.content == b"P"
    # Fetch the first two bytes.
    first_two_bytes_response = client.context.http_client.get(
        "/api/v1/asset/bytes/x?id=1",
        headers={"Range": "bytes=0-1"},
    )
    assert first_two_bytes_response.content == b"PA"
    # Fetch the second two bytes.
    second_two_bytes_response = client.context.http_client.get(
        "/api/v1/asset/bytes/x?id=1",
        headers={"Range": "bytes=2-3"},
    )
    assert second_two_bytes_response.content == b"R1"
    # Request outside of range
    out_of_range_response = client.context.http_client.get(
        "/api/v1/asset/bytes/x?id=1",
        headers={"Range": "bytes=1000000-100000000"},
    )
    with fail_with_status_code(HTTP_416_RANGE_NOT_SATISFIABLE):
        out_of_range_response.raise_for_status()
    # Request malformed range
    malformed_response = client.context.http_client.get(
        "/api/v1/asset/bytes/x?id=1",
        headers={"Range": "bytes=abc"},
    )
    with fail_with_status_code(HTTP_400_BAD_REQUEST):
        malformed_response.raise_for_status()


def test_get_asset_filepaths(client):
    "Smoke test get_asset_filepaths."
    client.write_array([1, 2, 3], key="x")
    get_asset_filepaths(client.include_data_sources()["x"])


def test_delete_internal_asset(client):
    """Deleting an internally-managed asset requires external_only=false and
    removes both the SQL record and the underlying data."""
    x = client.write_array([1, 2, 3], key="x")
    assets = x.include_data_sources().data_sources()[0].assets
    assert len(assets) == 1
    asset_id = assets[0].id
    filepaths = get_asset_filepaths(client.include_data_sources()["x"])
    assert all(path.exists() for path in filepaths)

    # By default (external_only=true) the server refuses to delete
    # internally-managed data.
    with fail_with_status_code(HTTP_409_CONFLICT):
        client.context.http_client.delete(
            f"/api/v1/asset/x?id={asset_id}"
        ).raise_for_status()
    # Nothing was deleted.
    assert all(path.exists() for path in filepaths)
    client["x"].read()

    # Opting in deletes the record and the underlying data.
    response = client.context.http_client.delete(
        f"/api/v1/asset/x?id={asset_id}&external_only=false"
    )
    response.raise_for_status()
    assert response.json() == {"asset_deleted": True, "data_deleted": True}
    assert not any(path.exists() for path in filepaths)

    # The node still exists, but the asset is gone.
    assets = x.include_data_sources().data_sources()[0].assets
    assert len(assets) == 0


def test_delete_asset_not_found(client):
    "Deleting a nonexistent asset id returns 404."
    client.write_array([1, 2, 3], key="x")
    with fail_with_status_code(HTTP_404_NOT_FOUND):
        client.context.http_client.delete(
            "/api/v1/asset/x?id=123456&external_only=false"
        ).raise_for_status()


def test_delete_asset_gated_by_expose_raw_assets(tmpdir):
    "With expose_raw_assets=False, deleting an asset returns 403."
    catalog = in_memory(writable_storage=str(tmpdir / "data"))
    app = build_app(catalog, server_settings={"expose_raw_assets": False})
    with Context.from_app(app) as context:
        client = from_context(context, include_data_sources=True)
        client.write_array([1, 2, 3], key="x")
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            client.context.http_client.delete(
                "/api/v1/asset/x?id=1&external_only=false"
            ).raise_for_status()


def test_do_not_expose_raw_assets(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir / "data"))
    app = build_app(catalog, server_settings={"expose_raw_assets": False})
    with Context.from_app(app) as context:
        client = from_context(context, include_data_sources=True)
        client.write_array([1, 2, 3], key="x")
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            client["x"].raw_export(tmpdir / "exported")


@pytest.mark.parametrize(
    "user_agent, expect_size_key",
    [
        # New client understands ``size``; server includes it.
        ("python-tiled/0.2.13", True),
        # Old client (pre-``size`` field) would crash on unknown kwarg; server strips it.
        ("python-tiled/0.2.12", False),
    ],
)
def test_asset_size_stripped_for_old_python_clients(
    client, user_agent, expect_size_key
):
    """The server strips ``size`` from assets when talking to python-tiled clients
    older than 0.2.13, because their ``Asset`` dataclass has no ``size`` field
    and ``DataSource.from_json`` unpacks kwargs directly, which would raise
    ``Asset.__init__() got an unexpected keyword argument 'size'``.
    """
    client.write_array([1, 2, 3], key="x")

    # GET path: /metadata/... with include_data_sources
    response = client.context.http_client.get(
        "/api/v1/metadata/x",
        params={"include_data_sources": "true"},
        headers={"user-agent": user_agent},
    )
    response.raise_for_status()
    data_sources = response.json()["data"]["attributes"]["data_sources"]
    assert data_sources, "expected at least one data_source"
    for ds in data_sources:
        for asset in ds["assets"]:
            assert ("size" in asset) is expect_size_key, (
                f"user-agent={user_agent!r}: expected size key "
                f"present={expect_size_key}, got asset={asset!r}"
            )
