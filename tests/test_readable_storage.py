"""
External files are served only from the server's readable storage.
"""

import sqlite3

import pandas
import pytest
import zarr
from starlette.status import HTTP_200_OK, HTTP_403_FORBIDDEN

from tiled.catalog import from_uri
from tiled.catalog.adapter import file_uris_outside_directories
from tiled.client import Context, from_context
from tiled.client.utils import ClientError
from tiled.server.app import build_app
from tiled.structures.array import ArrayStructure
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management
from tiled.structures.table import TableStructure
from tiled.utils import ensure_uri

from .utils import fail_with_status_code

df = pandas.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
SECRET = b"a,b\n7,8.0\n"

# Asset URIs that are not in readable storage, as functions of the test
# directory. A `file://` path is judged by where it leads. Without a scheme,
# some adapters read a URI as a path; with a scheme that this server has no
# storage for, some would fetch it from the network.
OUTSIDE_URIS = {
    "outside": lambda layout: ensure_uri(layout / "outside" / "secret.csv"),
    "dot-dot": lambda layout: ensure_uri(
        layout / "readable" / ".." / "outside" / "secret.csv"
    ),
    "link-to-file": lambda layout: ensure_uri(layout / "readable" / "link.csv"),
    "link-to-directory": lambda layout: ensure_uri(
        layout / "readable" / "linked" / "secret.csv"
    ),
    "no-scheme": lambda layout: str(layout / "readable" / "ok.csv"),
    "http": lambda layout: "http://example.invalid/bucket/ok.csv",
    "ftp": lambda layout: "ftp://example.invalid/secret.csv",
    "s3": lambda layout: "s3://bucket/secret.csv",
}


@pytest.fixture
def layout(tmp_path):
    """
    readable/            the readable storage
        ok.csv
        z/               a Zarr array
        link.csv         -> ../outside/secret.csv
        linked/          -> ../outside
    outside/
        secret.csv
        z/               a Zarr array
    """
    readable = tmp_path / "readable"
    outside = tmp_path / "outside"
    readable.mkdir()
    outside.mkdir()
    df.to_csv(readable / "ok.csv", index=False)
    (outside / "secret.csv").write_bytes(SECRET)
    for directory in (readable, outside):
        zarr.create_array(directory / "z", shape=(3,), dtype="i8")
    try:
        (readable / "link.csv").symlink_to(outside / "secret.csv")
        (readable / "linked").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Cannot create symbolic links on this platform.")
    return tmp_path


@pytest.fixture
def database(layout):
    return layout / "catalog.db"


@pytest.fixture
def client(layout, database):
    (layout / "writable").mkdir()
    catalog = from_uri(
        f"sqlite:///{database}",
        readable_storage=[layout / "readable"],
        writable_storage=[layout / "writable"],
        init_if_not_exists=True,
    )
    with Context.from_app(build_app(catalog)) as context:
        yield from_context(context)


def csv_data_source(data_uri):
    return DataSource(
        mimetype="text/csv;header=present",
        assets=[
            Asset(
                data_uri=data_uri,
                is_directory=False,
                parameter="data_uris",
                num=0,
            )
        ],
        structure_family=StructureFamily.table,
        structure=TableStructure.from_pandas(df),
        management=Management.external,
    )


def register_csv(client, key, data_uri):
    return client.new(
        structure_family=StructureFamily.table,
        data_sources=[csv_data_source(data_uri)],
        key=key,
    )


def register_zarr(client, key, path):
    return client.new(
        structure_family=StructureFamily.array,
        data_sources=[
            DataSource(
                mimetype="application/x-zarr",
                assets=[
                    Asset(
                        data_uri=ensure_uri(path),
                        is_directory=True,
                        parameter="data_uri",
                    )
                ],
                structure_family=StructureFamily.array,
                structure=ArrayStructure.from_array(zarr.open_array(path)),
                management=Management.external,
            )
        ],
        key=key,
    )


def asset_id(client, key):
    (asset,) = client[key].include_data_sources().data_sources()[0].assets
    return asset.id


def repoint_asset(database, old_path, new_uri, parameter="data_uri"):
    """Make an asset in the catalog name another location.

    With parameter=None the asset is not passed to the adapter."""
    with sqlite3.connect(database) as connection:
        cursor = connection.execute(
            "UPDATE assets SET data_uri = ? WHERE data_uri = ?",
            (new_uri, ensure_uri(old_path)),
        )
        assert cursor.rowcount == 1
        connection.execute(
            "UPDATE data_source_asset_association SET parameter = ? "
            "WHERE asset_id = (SELECT id FROM assets WHERE data_uri = ?)",
            (parameter, new_uri),
        )


def test_file_in_readable_storage_is_served(client, layout):
    register_csv(client, "ok", ensure_uri(layout / "readable" / "ok.csv"))

    assert client["ok"].read().equals(df)
    response = client.context.http_client.get(
        "/api/v1/asset/bytes/ok", params={"id": asset_id(client, "ok")}
    )
    assert response.status_code == HTTP_200_OK
    assert response.content == (layout / "readable" / "ok.csv").read_bytes()


@pytest.mark.parametrize("data_uri", OUTSIDE_URIS.values(), ids=OUTSIDE_URIS)
def test_asset_outside_readable_storage_cannot_be_registered(client, layout, data_uri):
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        register_csv(client, "x", data_uri(layout))

    assert "x" not in client


@pytest.mark.parametrize("data_uri", OUTSIDE_URIS.values(), ids=OUTSIDE_URIS)
def test_asset_outside_readable_storage_is_not_served(
    client, layout, database, data_uri
):
    "An asset that the catalog already holds is checked whenever it is read."
    register_csv(client, "x", ensure_uri(layout / "readable" / "ok.csv"))
    repoint_asset(
        database,
        layout / "readable" / "ok.csv",
        data_uri(layout),
        parameter="data_uris",
    )
    http_client = client.context.http_client

    data = http_client.get("/api/v1/table/full/x", params={"format": "text/csv"})
    raw = http_client.get("/api/v1/asset/bytes/x", params={"id": asset_id(client, "x")})

    for response in (data, raw):
        assert b"7,8.0" not in response.content
        assert response.status_code == HTTP_403_FORBIDDEN
    # The node can still be seen, and removed.
    assert "x" in client
    client.delete_contents("x")
    assert "x" not in client


@pytest.mark.parametrize("path", ["outside/z", "readable/linked/z"])
@pytest.mark.parametrize(
    "parameter", ["data_uri", None], ids=["passed-to-adapter", "supporting"]
)
def test_directory_outside_readable_storage_is_not_listed(
    client, layout, database, path, parameter
):
    """Also for an asset that is not passed to the adapter: some adapters open
    the first asset of a data source whatever its parameter."""
    register_zarr(client, "z", layout / "readable" / "z")
    http_client = client.context.http_client
    params = {"id": asset_id(client, "z")}
    manifest = http_client.get("/api/v1/asset/manifest/z", params=params)
    assert manifest.status_code == HTTP_200_OK
    assert "zarr.json" in manifest.json()["manifest"]
    repoint_asset(
        database, layout / "readable" / "z", ensure_uri(layout / path), parameter
    )

    manifest = http_client.get("/api/v1/asset/manifest/z", params=params)
    raw = http_client.get(
        "/api/v1/asset/bytes/z", params={**params, "relative_path": "zarr.json"}
    )

    assert manifest.status_code == HTTP_403_FORBIDDEN
    assert raw.status_code == HTTP_403_FORBIDDEN
    with pytest.raises(ClientError):
        client["z"].read()


@pytest.mark.parametrize(
    "data_uri, accepted",
    [
        ("http://storage.invalid/bucket/a/ok.csv", True),
        ("http://storage.invalid/other/ok.csv", False),
        ("http://elsewhere.invalid/bucket/ok.csv", False),
        ("https://storage.invalid/bucket/ok.csv", False),
    ],
    ids=["in-bucket", "other-bucket", "other-host", "other-scheme"],
)
def test_asset_in_readable_object_storage(database, data_uri, accepted):
    "An object storage URI is accepted in a readable bucket, and refused elsewhere."
    bucket = {"provider": "s3", "uri": "http://storage.invalid/bucket"}
    catalog = from_uri(
        f"sqlite:///{database}", readable_storage=[bucket], init_if_not_exists=True
    )
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)

        if accepted:
            register_csv(client, "x", data_uri)
        else:
            with fail_with_status_code(HTTP_403_FORBIDDEN):
                register_csv(client, "x", data_uri)
        assert ("x" in client) is accepted


def test_data_source_cannot_be_changed_to_file_outside_readable_storage(client, layout):
    register_csv(client, "x", ensure_uri(layout / "readable" / "ok.csv"))
    http_client = client.context.http_client
    (data_source,) = http_client.get(
        "/api/v1/metadata/x", params={"include_data_sources": "true"}
    ).json()["data"]["attributes"]["data_sources"]
    data_source["assets"][0]["data_uri"] = ensure_uri(layout / "outside" / "secret.csv")

    response = http_client.put(
        "/api/v1/data_source/x", json={"data_source": data_source}
    )

    assert response.status_code == HTTP_403_FORBIDDEN
    assert client["x"].read().equals(df)


def test_readable_storage_reached_through_a_link(tmp_path):
    "The storage directory may itself be a symbolic link."
    real = tmp_path / "real"
    real.mkdir()
    df.to_csv(real / "ok.csv", index=False)
    link = tmp_path / "link"
    try:
        link.symlink_to(real, target_is_directory=True)
    except OSError:
        pytest.skip("Cannot create symbolic links on this platform.")
    catalog = from_uri(
        f"sqlite:///{tmp_path / 'catalog.db'}",
        readable_storage=[link],
        init_if_not_exists=True,
    )
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        register_csv(client, "by_link", ensure_uri(link / "ok.csv"))
        register_csv(client, "by_real_path", ensure_uri(real / "ok.csv"))

        assert client["by_link"].read().equals(df)
        assert client["by_real_path"].read().equals(df)


def test_file_uris_outside_directories(layout):
    "Files that share a directory are judged one by one."
    readable = layout / "readable"
    for name in ("a.csv", "b.csv"):
        (readable / name).write_text("x\n1\n")
    names = ["a.csv", "link.csv", "b.csv", "missing.csv", "linked/secret.csv"]
    uris = [ensure_uri(readable / name) for name in names]
    uris.append("s3://bucket/key")  # Not a path. Never reported.

    outside = file_uris_outside_directories(uris, [readable])

    assert outside == [uris[1], uris[4]]
    assert file_uris_outside_directories(uris[:1], []) == uris[:1]
    # Both directories are readable.
    assert file_uris_outside_directories(uris, [readable, layout / "outside"]) == []
