import hashlib
from pathlib import Path

import httpx2 as httpx
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
from tiled.client.download import ATTACHMENT_FILENAME_PLACEHOLDER, download
from tiled.client.utils import ClientError, get_asset_filepaths
from tiled.server.app import build_app
from tiled.utils import UnsafePath, path_from_uri

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
    destination = Path(tmpdir / "exported")
    exported_paths = client["x"].raw_export(destination)
    data_sources = client["x"].include_data_sources().data_sources()
    orig_dir = path_from_uri(data_sources[0].assets[0].data_uri)
    _asset_id, relative_paths = client["x"].asset_manifest(data_sources).popitem()
    orig_paths = [Path(orig_dir, relative_path) for relative_path in relative_paths]
    orig_hashes = [hashlib.md5(path.read_bytes()).digest() for path in orig_paths]
    exported_hashes = [
        hashlib.md5(path.read_bytes()).digest() for path in exported_paths
    ]
    assert orig_hashes == exported_hashes
    # The files are copies in the destination, laid out as in the asset.
    assert relative_paths
    assert sorted(exported_paths) == sorted(
        destination / relative_path for relative_path in relative_paths
    )
    assert not set(exported_paths) & set(orig_paths)


def directory_asset(client, key="x"):
    "Write an array, stored as a directory. Return its asset id and directory."
    client.write_array([1, 2, 3], key=key)
    (asset,) = client[key].include_data_sources().data_sources()[0].assets
    assert asset.is_directory
    return asset.id, path_from_uri(asset.data_uri)


def test_asset_manifest_gives_relative_paths(client):
    "Each manifest entry is relative to the asset and can be passed to /asset/bytes."
    asset_id, directory = directory_asset(client)
    manifest = (
        client.context.http_client.get(f"/api/v1/asset/manifest/x?id={asset_id}")
        .raise_for_status()
        .json()["manifest"]
    )
    assert manifest
    for relative_path in manifest:
        assert not Path(relative_path).is_absolute()
        assert "\\" not in relative_path
        response = client.context.http_client.get(
            "/api/v1/asset/bytes/x",
            params={"id": asset_id, "relative_path": relative_path},
        ).raise_for_status()
        assert response.content == (directory / relative_path).read_bytes()


@pytest.mark.parametrize(
    "relative_path",
    [
        "../outside.txt",
        "c/../../outside.txt",
        "../x/../outside.txt",
        "{directory}/../outside.txt",
        "{outside}",
    ],
)
def test_asset_bytes_refuses_path_outside_asset(client, relative_path):
    "The relative_path parameter cannot name a file outside of the asset."
    asset_id, directory = directory_asset(client)
    outside = directory.parent / "outside.txt"
    outside.write_text("OUTSIDE")
    response = client.context.http_client.get(
        "/api/v1/asset/bytes/x",
        params={
            "id": asset_id,
            "relative_path": relative_path.format(directory=directory, outside=outside),
        },
    )
    assert b"OUTSIDE" not in response.content
    with fail_with_status_code(HTTP_400_BAD_REQUEST):
        response.raise_for_status()


@pytest.mark.parametrize("relative_path", ["no-such-file", "c", "c/0/no-such-file"])
def test_asset_bytes_of_missing_file(client, relative_path):
    "A path in the asset that is not a file is not found."
    asset_id, directory = directory_asset(client)
    assert (directory / "c").is_dir()

    response = client.context.http_client.get(
        "/api/v1/asset/bytes/x",
        params={"id": asset_id, "relative_path": relative_path},
    )

    assert response.status_code == HTTP_404_NOT_FOUND


def test_asset_bytes_refuses_symlink_leaving_asset(client):
    "A symbolic link in the asset is not followed out of the asset."
    asset_id, directory = directory_asset(client)
    outside = directory.parent / "outside.txt"
    outside.write_text("OUTSIDE")
    try:
        (directory / "link.txt").symlink_to(outside)
    except OSError:
        pytest.skip("Cannot create symbolic links on this platform.")
    response = client.context.http_client.get(
        "/api/v1/asset/bytes/x",
        params={"id": asset_id, "relative_path": "link.txt"},
    )
    assert b"OUTSIDE" not in response.content
    with fail_with_status_code(HTTP_400_BAD_REQUEST):
        response.raise_for_status()


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


@pytest.mark.parametrize(
    "filename",
    ["../../../outside.txt", "{outside}", "sub/inside.txt", "..", ""],
)
def test_download_refuses_filename_that_is_not_a_file_name(tmp_path, filename):
    "The server names a downloaded file, but does not choose its directory."
    destination = tmp_path / "exports" / "deep" / "directory"
    destination.mkdir(parents=True)
    filename = filename.format(outside=tmp_path / "outside.txt")

    def respond(request):
        return httpx.Response(
            200,
            content=b"DOWNLOADED",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as http_client:
        with pytest.raises(UnsafePath):
            download(
                http_client,
                ["http://server.invalid/api/v1/asset/bytes/x?id=1"],
                [destination / ATTACHMENT_FILENAME_PLACEHOLDER],
            )

    written = [
        path
        for path in tmp_path.rglob("*")
        if path.is_file() and path.read_bytes() == b"DOWNLOADED"
    ]
    assert not written


def test_download_names_file_as_the_server_says(tmp_path):
    def respond(request):
        return httpx.Response(
            200,
            content=b"DOWNLOADED",
            headers={"Content-Disposition": 'attachment; filename="data.bin"'},
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as http_client:
        (path,) = download(
            http_client,
            ["http://server.invalid/api/v1/asset/bytes/x?id=1"],
            [tmp_path / ATTACHMENT_FILENAME_PLACEHOLDER],
        )

    assert path == tmp_path / "data.bin"
    assert path.read_bytes() == b"DOWNLOADED"


@pytest.mark.parametrize(
    "entry", ["../outside.txt", "c/../../outside.txt", "{outside}", ".."]
)
@pytest.mark.parametrize("in_memory_destination", [False, True])
def test_raw_export_refuses_manifest_entry_outside_destination(
    client, tmp_path, monkeypatch, entry, in_memory_destination
):
    "A manifest from the server cannot place a file outside of the destination."
    asset_id, _directory = directory_asset(client)
    node = client["x"]
    entry = entry.format(outside=tmp_path / "outside.txt")
    monkeypatch.setattr(
        type(node), "asset_manifest", lambda self, data_sources: {asset_id: [entry]}
    )
    destination = {} if in_memory_destination else tmp_path / "exported"
    files_before = sorted(tmp_path.rglob("*"))

    with pytest.raises(UnsafePath):
        node.raw_export(destination)

    assert sorted(tmp_path.rglob("*")) == files_before
    assert not in_memory_destination or destination == {}


def test_download_raises_when_a_file_fails(tmp_path):
    "The other files are downloaded. The error is raised, not only logged."

    def respond(request):
        if request.url.path.endswith("missing"):
            return httpx.Response(404, json={"detail": "No such file"})
        return httpx.Response(200, content=b"DOWNLOADED")

    names = ["a", "missing", "b", "also-missing"]
    with httpx.Client(transport=httpx.MockTransport(respond)) as http_client:
        with pytest.raises(ClientError, match="404") as exc_info:
            download(
                http_client,
                [f"http://server.invalid/{name}" for name in names],
                [tmp_path / name for name in names],
            )

    assert exc_info.value.__notes__ == [
        "2 of 4 downloads failed. This is the first error."
    ]
    assert sorted(path.name for path in tmp_path.iterdir()) == ["a", "b"]


def test_download_removes_file_that_was_written_in_part(tmp_path):
    class Truncated(httpx.SyncByteStream):
        def __iter__(self):
            yield b"PART"
            raise RuntimeError("connection lost")

    def respond(request):
        return httpx.Response(200, stream=Truncated())

    with httpx.Client(transport=httpx.MockTransport(respond)) as http_client:
        with pytest.raises(RuntimeError, match="connection lost"):
            download(http_client, ["http://server.invalid/x"], [tmp_path / "x"])

    assert list(tmp_path.iterdir()) == []


def test_raw_export_raises_when_a_file_cannot_be_downloaded(
    client, tmp_path, monkeypatch
):
    "An export that is not complete does not return as if it were."
    asset_id, _directory = directory_asset(client)
    node = client["x"]
    monkeypatch.setattr(
        type(node),
        "asset_manifest",
        lambda self, data_sources: {asset_id: ["zarr.json", "no-such-file"]},
    )

    with pytest.raises(ClientError):
        node.raw_export(tmp_path / "exported")

    assert sorted(path.name for path in (tmp_path / "exported").iterdir()) == [
        "zarr.json"
    ]
