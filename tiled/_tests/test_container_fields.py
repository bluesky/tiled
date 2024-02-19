import io

import h5py
import pandas
import pytest
import zarr

from ..catalog import in_memory
from ..catalog.register import register
from ..client import Context, from_context, record_history
from ..examples.generate_files import generate_files
from ..server.app import build_app


def awaitable_client_from_data_dir(data_dir):
    "Parametrize a test to use a tiled client that serves from data_dir (Path or str)"

    def decorator(test_func):
        return pytest.mark.parametrize(
            "awaitable_client_generator", (data_dir,), indirect=True
        )(test_func)

    return decorator


@pytest.fixture
async def awaitable_client_generator(request: pytest.FixtureRequest):
    data_dir = request.getfixturevalue(request.param)
    catalog = in_memory(readable_storage=[data_dir])
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(catalog, data_dir)
        yield client


@pytest.fixture
async def awaitable_client(awaitable_client_generator):
    return await awaitable_client_generator.__anext__()


@pytest.fixture
def example_data_dir(tmpdir_factory):
    """
    Generate a temporary directory with example files.

    The tmpdir_factory fixture ensures that this directory is cleaned up at test exit.
    """
    tmpdir = tmpdir_factory.mktemp("example_files")
    generate_files(tmpdir)
    return tmpdir


@pytest.mark.asyncio
@pytest.mark.parametrize("fields", (None, (), ("a", "b")))
@awaitable_client_from_data_dir("example_data_dir")
async def test_directory_fields(awaitable_client, fields):
    client = await awaitable_client
    url_path = client.item["links"]["full"]
    buffer = io.BytesIO()
    with record_history() as history:
        client.export(buffer, fields=fields, format="application/x-hdf5")

    # Directory contents were fetched as a single request using "full" route
    (request,) = history.requests
    request_url_path = request.url.copy_with(query=None)
    assert request_url_path == url_path

    # Only the requested fields were fetched
    file = h5py.File(buffer, "r")
    actual_fields = set(file.keys())
    expected = set(fields or client.keys())  # By default all fields were fetched
    assert actual_fields == expected


@pytest.fixture
def excel_data_dir(tmpdir):
    df = pandas.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    with pandas.ExcelWriter(tmpdir / "spreadsheet.xlsx") as writer:
        for i in range(10):
            df.to_excel(writer, sheet_name=f"Sheet {i+1}", index=False)
    return tmpdir


@pytest.mark.asyncio
@pytest.mark.parametrize("fields", (None, (), ("Sheet 1", "Sheet 10")))
@awaitable_client_from_data_dir("excel_data_dir")
async def test_excel_fields(awaitable_client, fields):
    client = await awaitable_client
    client = client["spreadsheet"]
    url_path = client.item["links"]["full"]
    buffer = io.BytesIO()
    with record_history() as history:
        client.export(buffer, fields=fields, format="application/x-hdf5")
        # TODO: Enable container to export XLSX if all nodes are tables?
        # client.export(buffer, fields=fields, format=XLSX_MIME_TYPE)

    # Spreadsheet contents were fetched as a single request using "full" route
    for request in history.requests:
        print(request)
    (request,) = history.requests
    request_url_path = request.url.copy_with(query=None)
    assert request_url_path == url_path

    # Only the requested fields were fetched
    file = h5py.File(buffer)
    actual_fields = set(file.keys())
    expected = set(fields or client.keys())  # By default all fields were fetched
    assert actual_fields == expected


def mark_xfail(value, unsupported="UNSPECIFIED ADAPTER"):
    "Indicate that this parameterized value is expected to fail"
    reason = "Tiled does not currently support selecting 'fields' for {unsupported}"
    return pytest.param(value, marks=pytest.mark.xfail(reason=reason))


@pytest.fixture
def zarr_data_dir(tmpdir):
    root = zarr.open(str(tmpdir / "zarr_group.zarr"), "w")
    for i, group in enumerate("abcde"):
        root.create_dataset(group, data=range(i, i + 3))
    return tmpdir


@pytest.mark.asyncio
@pytest.mark.parametrize("fields", (None, (), mark_xfail(("b", "d"), "Zarr")))
@awaitable_client_from_data_dir("zarr_data_dir")
async def test_zarr_group_fields(awaitable_client, fields):
    client = await awaitable_client
    client = client["zarr_group"]
    url_path = client.item["links"]["full"]
    buffer = io.BytesIO()
    with record_history() as history:
        client.export(buffer, fields=fields, format="application/x-hdf5")

    # Spreadsheet contents were fetched as a single request using "full" route
    for request in history.requests:
        print(request)
    (request,) = history.requests
    request_url_path = request.url.copy_with(query=None)
    assert request_url_path == url_path

    # Only the requested fields were fetched
    file = h5py.File(buffer)
    actual_fields = set(file.keys())
    expected = set(fields or client.keys())  # By default all fields were fetched
    assert actual_fields == expected


@pytest.fixture
def hdf5_data_dir(tmpdir):
    with h5py.File(str(tmpdir / "hdf5_example.h5"), "w") as file:
        file["x"] = [1, 2, 3]
        group = file.create_group("g")
        group["y"] = [4, 5, 6]
    return tmpdir


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields", (None, (), mark_xfail(("x",), "HDF5"), mark_xfail(("g",), "HDF5"))
)
@awaitable_client_from_data_dir("hdf5_data_dir")
async def test_hdf5_fields(awaitable_client, fields):
    client = await awaitable_client
    client = client["hdf5_example"]
    url_path = client.item["links"]["full"]
    buffer = io.BytesIO()
    with record_history() as history:
        client.export(buffer, fields=fields, format="application/x-hdf5")

    # Spreadsheet contents were fetched as a single request using "full" route
    for request in history.requests:
        print(request)
    (request,) = history.requests
    request_url_path = request.url.copy_with(query=None)
    assert request_url_path == url_path

    # Only the requested fields were fetched
    file = h5py.File(buffer)
    actual_fields = set(file.keys())
    expected = set(fields or client.keys())  # By default all fields were fetched
    assert actual_fields == expected
