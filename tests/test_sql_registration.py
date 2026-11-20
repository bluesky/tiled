"""
Tables in SQL storage are written by the server, which chooses where each
one's rows are. A client cannot register a table in SQL storage.
"""

import json
import sqlite3

import numpy
import pandas
import pyarrow
import pytest
from starlette.status import (
    HTTP_200_OK,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_422_UNPROCESSABLE_CONTENT,
)

from tiled.catalog import from_uri
from tiled.client import Context, from_context
from tiled.client.register import register
from tiled.server.app import build_app
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management
from tiled.structures.table import TableStructure
from tiled.utils import ensure_uri

schema = pyarrow.schema([("secret", pyarrow.string())])
df = pandas.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})


@pytest.fixture
def layout(tmp_path):
    for name in ("readable", "outside"):
        (tmp_path / name).mkdir()
    df.to_csv(tmp_path / "readable" / "ok.csv", index=False)
    (tmp_path / "outside" / "secret.csv").write_text("a,b\n7,8.0\n")
    return tmp_path


@pytest.fixture(params=["duckdb", "sqlite"])
def client(layout, request):
    catalog = from_uri(
        f"sqlite:///{layout / 'catalog.db'}",
        readable_storage=[layout / "readable"],
        writable_storage=[
            layout / "writable",
            f"{request.param}:///{layout / 'storage.db'}",
        ],
        init_if_not_exists=True,
    )
    with Context.from_app(build_app(catalog)) as context:
        yield from_context(context)


@pytest.fixture
def tables(client):
    "Two tables in SQL storage: `theirs`, and `mine`."
    theirs = client.create_appendable_table(schema, key="theirs")
    theirs.append_partition(0, pyarrow.table({"secret": ["their-1", "their-2"]}))
    mine = client.create_appendable_table(schema, key="mine")
    mine.append_partition(0, pyarrow.table({"secret": ["my-own"]}))
    return theirs, mine


def stored_data_source(client, key):
    (data_source,) = (
        client.context.http_client.get(
            f"/api/v1/metadata/{key}", params={"include_data_sources": "true"}
        )
        .raise_for_status()
        .json()["data"]["attributes"]["data_sources"]
    )
    return data_source


def rows(client, key):
    return client[key].read()["secret"].tolist()


def registration(key, mine, parameters):
    "A request to register the storage of `mine` again, with other parameters."
    return {
        "id": key,
        "structure_family": "table",
        "metadata": {},
        "specs": [],
        "data_sources": [
            {
                "structure_family": "table",
                "structure": mine["structure"],
                "mimetype": mine["mimetype"],
                "management": "external",
                "parameters": parameters,
                "assets": [
                    {
                        "data_uri": asset["data_uri"],
                        "is_directory": asset["is_directory"],
                        "parameter": asset["parameter"],
                        "num": asset["num"],
                    }
                    for asset in mine["assets"]
                ],
            }
        ],
    }


@pytest.mark.parametrize(
    "dataset_id", ["theirs", "-1 OR 1=1"], ids=["another-dataset", "injection"]
)
def test_table_in_sql_storage_cannot_be_registered(client, tables, dataset_id):
    mine, theirs = (
        stored_data_source(client, "mine"),
        stored_data_source(client, "theirs"),
    )
    parameters = dict(mine["parameters"])
    if dataset_id == "theirs":
        parameters["dataset_id"] = theirs["parameters"]["dataset_id"]
    else:
        parameters["dataset_id"] = dataset_id

    response = client.context.http_client.post(
        "/api/v1/register/", json=registration("forged", mine, parameters)
    )

    assert response.status_code == HTTP_403_FORBIDDEN
    assert "forged" not in client


def test_parameters_of_table_in_sql_storage_cannot_be_changed(client, tables):
    mine, theirs = (
        stored_data_source(client, "mine"),
        stored_data_source(client, "theirs"),
    )
    for management in ("writable", "external"):
        changed = {
            **mine,
            "management": management,
            "parameters": theirs["parameters"],
        }

        response = client.context.http_client.put(
            "/api/v1/data_source/mine", json={"data_source": changed}
        )

        assert response.status_code in (HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND)
    assert rows(client, "mine") == ["my-own"]


def test_external_table_already_in_catalog_is_not_read_or_emptied(
    client, tables, layout
):
    """A catalog may hold an external table in SQL storage that names the rows
    of another table."""
    mine, theirs = (
        stored_data_source(client, "mine"),
        stored_data_source(client, "theirs"),
    )
    with sqlite3.connect(layout / "catalog.db") as connection:
        cursor = connection.execute(
            "UPDATE data_sources SET management = 'external', parameters = ? "
            "WHERE id = ?",
            (json.dumps(theirs["parameters"]), mine["id"]),
        )
        assert cursor.rowcount == 1
    http_client = client.context.http_client

    response = http_client.get("/api/v1/table/full/mine", params={"format": "text/csv"})
    assert response.status_code == HTTP_403_FORBIDDEN
    assert b"their" not in response.content

    # The node can be removed. The rows it named are left alone.
    assert http_client.delete("/api/v1/metadata/mine").status_code == HTTP_200_OK
    assert "mine" not in client
    assert rows(client, "theirs") == ["their-1", "their-2"]


def test_managed_table_with_forged_parameters_already_in_catalog_is_not_read(
    client, tables, layout
):
    "Parameters of a managed table that are not identifiers are not put into SQL."
    mine = stored_data_source(client, "mine")
    forged = {**mine["parameters"], "dataset_id": "-1 OR 1=1"}
    with sqlite3.connect(layout / "catalog.db") as connection:
        cursor = connection.execute(
            "UPDATE data_sources SET parameters = ? WHERE id = ?",
            (json.dumps(forged), mine["id"]),
        )
        assert cursor.rowcount == 1

    response = client.context.http_client.get(
        "/api/v1/table/full/mine", params={"format": "text/csv"}
    )

    assert response.status_code == HTTP_422_UNPROCESSABLE_CONTENT
    assert b"their" not in response.content
    assert rows(client, "theirs") == ["their-1", "their-2"]


def test_table_written_by_the_server_is_read_and_removed(client, tables):
    assert rows(client, "theirs") == ["their-1", "their-2"]
    assert rows(client, "mine") == ["my-own"]

    client.delete_contents("mine", external_only=False)

    assert "mine" not in client
    assert rows(client, "theirs") == ["their-1", "their-2"]
    # The rows are gone, so a new table does not find them.
    again = client.create_appendable_table(schema, key="again")
    assert rows(client, "again") == []
    again.append_partition(0, pyarrow.table({"secret": ["new"]}))
    assert rows(client, "again") == ["new"]


def csv_data_source(data_uri):
    return DataSource(
        mimetype="text/csv;header=present",
        assets=[
            Asset(data_uri=data_uri, is_directory=False, parameter="data_uris", num=0)
        ],
        structure_family=StructureFamily.table,
        structure=TableStructure.from_pandas(df),
        management=Management.external,
    )


@pytest.mark.parametrize("scheme", ["sqlite", "duckdb"])
def test_file_cannot_be_registered_under_a_database_scheme(client, layout, scheme):
    "A URI with the scheme of a database also names a path."
    data_uri = f"{scheme}:///{layout / 'outside' / 'secret.csv'}"
    request = registration(
        "x",
        {
            "structure": TableStructure.from_pandas(df).__dict__
            | {"arrow_schema": TableStructure.from_pandas(df).arrow_schema},
            "mimetype": "text/csv;header=present",
            "assets": [
                {
                    "data_uri": data_uri,
                    "is_directory": False,
                    "parameter": "data_uris",
                    "num": 0,
                }
            ],
        },
        {},
    )

    response = client.context.http_client.post("/api/v1/register/", json=request)

    assert response.status_code == HTTP_403_FORBIDDEN
    assert "x" not in client


@pytest.mark.parametrize("scheme", ["sqlite", "duckdb"])
@pytest.mark.parametrize("kind", ["file", "sequence"])
async def test_file_under_a_database_scheme_already_in_catalog_is_not_read(
    client, layout, scheme, kind
):
    "A sequence of files is read one file at a time, by a path of its own."
    readable = layout / "readable"
    if kind == "file":
        client.new(
            structure_family=StructureFamily.table,
            data_sources=[csv_data_source(ensure_uri(readable / "ok.csv"))],
            key="x",
        )
        assert client["x"].read().equals(df)
        repointed, route = readable / "ok.csv", "table/full/x?format=text/csv"
    else:
        tifffile = pytest.importorskip("tifffile")
        for i in range(2):
            tifffile.imwrite(readable / f"image{i:05}.tif", numpy.full((2, 2), i, "u1"))
        await register(client, readable)
        assert client["image"].read().shape == (2, 2, 2)
        repointed, route = readable / "image00001.tif", "array/block/image?block=1,0,0"
    with sqlite3.connect(layout / "catalog.db") as connection:
        cursor = connection.execute(
            "UPDATE assets SET data_uri = ? WHERE data_uri = ?",
            (f"{scheme}:///{layout / 'outside' / 'secret.csv'}", ensure_uri(repointed)),
        )
        assert cursor.rowcount == 1

    response = client.context.http_client.get(f"/api/v1/{route}")

    assert response.status_code == HTTP_403_FORBIDDEN
    assert b"7,8.0" not in response.content
