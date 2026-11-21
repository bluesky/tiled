"""
External data is registered to be read. The server does not write to it.
"""

import awkward
import numpy
import pandas
import pytest
import zarr
from starlette.status import HTTP_200_OK, HTTP_405_METHOD_NOT_ALLOWED

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.structures.array import ArrayStructure
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management
from tiled.structures.table import TableStructure
from tiled.utils import ensure_uri

from .utils import fail_with_status_code

df = pandas.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
more = pandas.DataFrame({"a": [7], "b": [8.0]})


def state(directory):
    "The content of every file in a directory."
    return {
        str(path.relative_to(directory)): path.read_bytes()
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


@pytest.fixture(params=[False, True], ids=["no-streaming", "streaming"])
def streaming(request):
    return request.param


@pytest.fixture
def external(tmp_path):
    directory = tmp_path / "external"
    directory.mkdir()
    return directory


@pytest.fixture
def client(tmp_path, external, streaming):
    catalog = in_memory(
        writable_storage=[
            str(tmp_path / "writable"),
            f"duckdb:///{tmp_path / 'data.duckdb'}",
        ],
        readable_storage=[external],
        cache_config=(
            {"uri": "memory://", "data_ttl": 60, "seq_ttl": 60} if streaming else None
        ),
    )
    with Context.from_app(build_app(catalog)) as context:
        yield from_context(context)


@pytest.fixture
def table(client, external):
    "A registered CSV file."
    df.to_csv(external / "table.csv", index=False)
    return client.new(
        structure_family=StructureFamily.table,
        data_sources=[
            DataSource(
                mimetype="text/csv;header=present",
                assets=[
                    Asset(
                        data_uri=ensure_uri(external / "table.csv"),
                        is_directory=False,
                        parameter="data_uris",
                        num=0,
                    )
                ],
                structure_family=StructureFamily.table,
                structure=TableStructure.from_pandas(df),
                management=Management.external,
            )
        ],
        key="table",
    )


@pytest.fixture
def array(client, external):
    "A registered Zarr array."
    stored = zarr.create_array(external / "array", shape=(4,), chunks=(2,), dtype="i8")
    stored[:] = numpy.arange(4)
    return client.new(
        structure_family=StructureFamily.array,
        data_sources=[
            DataSource(
                mimetype="application/x-zarr",
                assets=[
                    Asset(
                        data_uri=ensure_uri(external / "array"),
                        is_directory=True,
                        parameter="data_uri",
                    )
                ],
                structure_family=StructureFamily.array,
                structure=ArrayStructure.from_array(stored),
                management=Management.external,
            )
        ],
        key="array",
    )


@pytest.fixture
def awkward_array(client, external):
    "A registered directory of awkward buffers."
    data = awkward.Array([[1, 2], [3]])
    form, length, container = awkward.to_buffers(data)
    (external / "awkward").mkdir()
    for key, value in container.items():
        (external / "awkward" / key).write_bytes(numpy.asarray(value).tobytes())
    return client.new(
        structure_family=StructureFamily.awkward,
        data_sources=[
            DataSource(
                mimetype="application/x-awkward-buffers",
                assets=[
                    Asset(
                        data_uri=ensure_uri(external / "awkward"),
                        is_directory=True,
                        parameter="data_uri",
                    )
                ],
                structure_family=StructureFamily.awkward,
                structure={"length": length, "form": form.to_dict()},
                management=Management.external,
            )
        ],
        key="awkward",
    )


@pytest.mark.parametrize(
    "write",
    [
        lambda node: node.write(more),
        lambda node: node.write_partition(0, more),
        lambda node: node.append_partition(0, more),
    ],
    ids=["write", "write_partition", "append_partition"],
)
def test_external_table_is_not_written(table, external, write):
    before = state(external)

    with fail_with_status_code(HTTP_405_METHOD_NOT_ALLOWED):
        write(table)

    assert state(external) == before
    assert table.read().equals(df)


@pytest.mark.parametrize(
    "write",
    [
        lambda node: node.write(numpy.arange(4) + 10),
        lambda node: node.write_block(numpy.arange(2) + 10, block=(0,)),
        lambda node: node.patch(numpy.arange(2) + 10, offset=(0,)),
        lambda node: node.patch(numpy.arange(2) + 10, offset=(4,), extend=True),
    ],
    ids=["write", "write_block", "patch", "patch-extend"],
)
def test_external_array_is_not_written(array, external, write):
    before = state(external)

    with fail_with_status_code(HTTP_405_METHOD_NOT_ALLOWED):
        write(array)

    assert state(external) == before
    assert (array.read() == numpy.arange(4)).all()


def test_external_awkward_array_is_not_written(client, awkward_array, external):
    before = state(external)

    _form, _length, container = awkward.to_buffers(awkward.Array([[9, 9], [9]]))
    with fail_with_status_code(HTTP_405_METHOD_NOT_ALLOWED):
        awkward_array.write(container)

    assert state(external) == before
    assert awkward.to_list(awkward_array.read()) == [[1, 2], [3]]


def test_update_can_be_streamed_without_writing_external_array(
    client, array, external, streaming
):
    "An update that is only streamed does not touch the data, so it is allowed."
    before = state(external)

    response = client.context.http_client.put(
        "/api/v1/array/full/array",
        params={"persist": "false"},
        content=(numpy.arange(4) + 10).tobytes(),
        headers={"Content-Type": "application/octet-stream"},
    )

    assert response.status_code == HTTP_200_OK
    assert state(external) == before


def test_data_written_through_the_server_can_be_written_again(client):
    array = client.write_array(numpy.arange(4), key="internal_array")
    array.write(numpy.arange(4) + 10)
    assert (array.read() == numpy.arange(4) + 10).all()

    table = client.write_table(df, key="internal_table")
    table.write_partition(0, more)
    assert table.read().equals(more)
