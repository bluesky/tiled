"""
A client that registers data names where the data is. Such a location is only
ever read: the data source is external, and stays external.
"""

import sqlite3

import numpy
import pandas
import pytest
import zarr
from starlette.status import (
    HTTP_200_OK,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_405_METHOD_NOT_ALLOWED,
)

from tiled.catalog import from_uri
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.structures.array import ArrayStructure
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management
from tiled.structures.table import TableStructure
from tiled.utils import ensure_uri, path_from_uri

from .utils import fail_with_status_code

df = pandas.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})


@pytest.fixture
def layout(tmp_path):
    readable = tmp_path / "readable"
    readable.mkdir()
    for name in ("one.csv", "two.csv"):
        df.to_csv(readable / name, index=False)
    return tmp_path


@pytest.fixture
def client(layout):
    catalog = from_uri(
        f"sqlite:///{layout / 'catalog.db'}",
        readable_storage=[layout / "readable"],
        writable_storage=[layout / "writable"],
        init_if_not_exists=True,
    )
    with Context.from_app(build_app(catalog)) as context:
        yield from_context(context)


def csv_data_source(path, management=Management.external):
    return DataSource(
        mimetype="text/csv;header=present",
        assets=[
            Asset(
                data_uri=ensure_uri(path),
                is_directory=False,
                parameter="data_uris",
                num=0,
            )
        ],
        structure_family=StructureFamily.table,
        structure=TableStructure.from_pandas(df),
        management=management,
    )


def register(client, key, path, management=Management.external):
    return client.new(
        structure_family=StructureFamily.table,
        data_sources=[csv_data_source(path, management)],
        key=key,
    )


def stored_data_source(client, key):
    "The data source of a node, as the server gives it."
    (data_source,) = (
        client.context.http_client.get(
            f"/api/v1/metadata/{key}", params={"include_data_sources": "true"}
        )
        .raise_for_status()
        .json()["data"]["attributes"]["data_sources"]
    )
    return data_source


def put_data_source(client, key, data_source):
    return client.context.http_client.put(
        f"/api/v1/data_source/{key}", json={"data_source": data_source}
    )


def test_registered_data_is_external(client, layout):
    node = register(client, "x", layout / "readable" / "one.csv")

    assert stored_data_source(client, "x")["management"] == "external"
    assert node.read().equals(df)


@pytest.mark.parametrize("management", ["writable", "immutable", "locked"])
@pytest.mark.parametrize("parameter", ["data_uri", None])
def test_location_cannot_be_registered_as_managed_by_the_server(
    client, layout, management, parameter
):
    """Else the server would remove the directory together with the node.

    An asset with no parameter is one that the adapter does not open, such as
    a file that another file refers to.
    """
    directory = layout / "readable" / "array"
    stored = zarr.create_array(directory, shape=(4,), chunks=(2,), dtype="i8")
    data_source = DataSource(
        mimetype="application/x-zarr",
        assets=[
            Asset(
                data_uri=ensure_uri(directory), is_directory=True, parameter=parameter
            )
        ],
        structure_family=StructureFamily.array,
        structure=ArrayStructure.from_array(stored),
        management=Management(management),
    )

    with fail_with_status_code(HTTP_403_FORBIDDEN):
        client.new(
            structure_family=StructureFamily.array, data_sources=[data_source], key="x"
        )

    assert "x" not in client
    assert (directory / "zarr.json").exists()


@pytest.mark.parametrize("management", ["writable", "immutable", "locked"])
def test_external_data_source_cannot_be_made_managed(client, layout, management):
    path = layout / "readable" / "one.csv"
    register(client, "x", path)
    data_source = stored_data_source(client, "x")
    data_source["management"] = management

    response = put_data_source(client, "x", data_source)

    assert response.status_code == HTTP_403_FORBIDDEN
    assert stored_data_source(client, "x")["management"] == "external"
    with fail_with_status_code(HTTP_405_METHOD_NOT_ALLOWED):
        client["x"].write_partition(0, df.iloc[:1])
    client.delete_contents("x", external_only=False)
    assert path.read_bytes() == (layout / "readable" / "two.csv").read_bytes()


def test_data_source_written_by_the_server_cannot_be_updated(client, layout):
    "Its location and parameters are the server's own."
    client.write_table(df, key="internal")
    data_source = stored_data_source(client, "internal")
    for management in ("writable", "external"):
        changed = {
            **data_source,
            "management": management,
            "parameters": {"changed": True},
        }

        response = put_data_source(client, "internal", changed)

        assert response.status_code in (HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND)
        assert stored_data_source(client, "internal") == data_source
    assert client["internal"].read().equals(df)


def test_external_data_source_can_be_updated(client, layout):
    register(client, "x", layout / "readable" / "one.csv")
    data_source = stored_data_source(client, "x")
    data_source["properties"] = {"note": "updated"}

    response = put_data_source(client, "x", data_source)

    assert response.status_code == HTTP_200_OK
    assert stored_data_source(client, "x")["properties"] == {"note": "updated"}


def test_data_source_of_another_node_cannot_be_updated(client, layout):
    "The data source is looked up on the node that the request names."
    register(client, "mine", layout / "readable" / "one.csv")
    register(client, "theirs", layout / "readable" / "two.csv")
    theirs = stored_data_source(client, "theirs")
    forged = {**stored_data_source(client, "mine"), "id": theirs["id"]}
    forged["properties"] = {"note": "forged"}
    forged["assets"] = []

    response = put_data_source(client, "mine", forged)

    assert response.status_code == HTTP_404_NOT_FOUND
    assert stored_data_source(client, "theirs") == theirs


def test_node_is_removed_without_removing_what_is_outside_writable_storage(
    client, layout
):
    """A catalog may hold a location outside of writable storage that is marked
    as managed by the server. Removing the node leaves the file."""
    path = layout / "readable" / "one.csv"
    client.write_array(numpy.arange(3), key="x")
    (internal,) = client["x"].include_data_sources().data_sources()[0].assets
    with sqlite3.connect(layout / "catalog.db") as connection:
        cursor = connection.execute(
            "UPDATE assets SET data_uri = ?, is_directory = 0 WHERE id = ?",
            (ensure_uri(path), internal.id),
        )
        assert cursor.rowcount == 1

    client.delete_contents("x", external_only=False)

    assert "x" not in client
    assert path.exists()


def test_data_written_by_the_server_is_removed_with_the_node(client, layout):
    client.write_array(numpy.arange(3), key="x")
    (asset,) = client["x"].include_data_sources().data_sources()[0].assets
    directory = path_from_uri(asset.data_uri)
    assert directory.is_relative_to(layout / "writable")
    assert directory.exists()

    client.delete_contents("x", external_only=False)

    assert not directory.exists()
