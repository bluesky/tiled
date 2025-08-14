import anyio
import h5py
import numpy as np
import pandas
import pytest
import zarr

from ..adapters.zarr import ZARR_LIB_V2
from ..catalog import in_memory
from ..client import Context, from_context, record_history
from ..client.register import register
from ..examples.generate_files import generate_files
from ..server.app import build_app


@pytest.fixture(scope="module")
def client(request: pytest.FixtureRequest):
    "A tiled client that serves from parametrized data_dir (Path or str)"
    data_dir = request.getfixturevalue(request.param)
    catalog = in_memory(readable_storage=[data_dir])
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        anyio.run(register, client, data_dir)
        yield client


@pytest.fixture(scope="module")
def example_data_dir(tmpdir_factory):
    "Generate a temporary directory with example files."
    tmpdir = tmpdir_factory.mktemp("example_files")
    generate_files(tmpdir)
    return tmpdir


@pytest.mark.parametrize("fields", (None, (), ("a", "b")))
@pytest.mark.parametrize("client", ("example_data_dir",), indirect=True)
def test_directory_fields(client, fields, buffer):
    "Export selected fields (files) from a directory via /container/full."
    url_path = client.item["links"]["full"]
    with record_history() as history:
        client.export(buffer, fields=fields, format="application/x-hdf5")

    assert_single_request_to_url(history, url_path)
    assert_requested_fields_fetched(buffer, fields, client)


@pytest.fixture(scope="module")
def excel_data_dir(tmpdir_factory):
    "Generate a temporary Excel file with multiple Sheets of tabular data."
    tmpdir = tmpdir_factory.mktemp("excel_files")
    df = pandas.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    with pandas.ExcelWriter(tmpdir / "spreadsheet.xlsx") as writer:
        for i in range(10):
            df.to_excel(writer, sheet_name=f"Sheet {i+1}", index=False)
    return tmpdir


@pytest.mark.parametrize("fields", (None, (), ("Sheet 1", "Sheet 10")))
@pytest.mark.parametrize("client", ("excel_data_dir",), indirect=True)
def test_excel_fields(client, fields, buffer):
    "Export selected fields (sheets) from an Excel file via /container/full."
    client = client["spreadsheet"]
    url_path = client.item["links"]["full"]
    with record_history() as history:
        client.export(buffer, fields=fields, format="application/x-hdf5")
        # TODO: Enable container to export XLSX if all nodes are tables?
        # client.export(buffer, fields=fields, format=XLSX_MIME_TYPE)

    assert_single_request_to_url(history, url_path)
    assert_requested_fields_fetched(buffer, fields, client)


def mark_xfail(value, unsupported="UNSPECIFIED ADAPTER"):
    "Indicate that this parameterized value is expected to fail"
    reason = f"Tiled does not currently support selecting 'fields' for {unsupported}"
    return pytest.param(value, marks=pytest.mark.xfail(reason=reason))


@pytest.fixture(scope="module")
def zarr_data_dir(tmpdir_factory):
    "Generate a temporary Zarr group file with multiple datasets."
    tmpdir = tmpdir_factory.mktemp("zarr_files")
    try:
        root = zarr.open(str(tmpdir / "zarr_group.zarr"), mode="w")
        for i, name in enumerate("abcde"):
            if ZARR_LIB_V2:
                root.create_dataset(name, data=np.arange(i, i + 3))
            else:
                root.create_array(name, data=np.arange(i, i + 3))
    finally:
        # Ensure the Zarr group is closed properly
        print("Closed")
    return tmpdir


@pytest.mark.parametrize("fields", (None, (), mark_xfail(("b", "d"), "Zarr")))
@pytest.mark.parametrize("client", ("zarr_data_dir",), indirect=True)
def test_zarr_group_fields(client, fields, buffer):
    "Export selected fields (Datasets) from a Zarr group via /container/full."
    client = client["zarr_group"]
    # Normally, zarr would have 'attributes' stored as internal dictionary in the
    # metadata, but HDF5 does not support nested dictionaries.
    client.replace_metadata({"attributes": "", "zarr_format": 2 if ZARR_LIB_V2 else 3})
    url_path = client.item["links"]["full"]
    with record_history() as history:
        client.export(buffer, fields=fields, format="application/x-hdf5")

        assert_single_request_to_url(history, url_path)
        assert_requested_fields_fetched(buffer, fields, client)


@pytest.fixture(scope="module")
def hdf5_data_dir(tmpdir_factory):
    "Generate a temporary HDF5 file with multiple entries."
    tmpdir = tmpdir_factory.mktemp("hdf5_files")
    with h5py.File(str(tmpdir / "hdf5_example.h5"), "w") as file:
        file["x"] = [1, 2, 3]
        group = file.create_group("g")
        group["y"] = [4, 5, 6]
    return tmpdir


@pytest.mark.parametrize(
    "fields", (None, (), mark_xfail(("x",), "HDF5"), mark_xfail(("g",), "HDF5"))
)
@pytest.mark.parametrize("client", ("hdf5_data_dir",), indirect=True)
def test_hdf5_fields(client, fields, buffer):
    "Export selected fields (array/group) from a HDF5 file via /container/full."
    client = client["hdf5_example"]
    url_path = client.item["links"]["full"]
    with record_history() as history:
        client.export(buffer, fields=fields, format="application/x-hdf5")

    assert_single_request_to_url(history, url_path)
    assert_requested_fields_fetched(buffer, fields, client)


def assert_single_request_to_url(history, url_path):
    "Container contents were fetched as a single request using 'full' route."
    (request,) = history.requests
    request_url_path = request.url.copy_with(query=None)
    assert request_url_path == url_path


def assert_requested_fields_fetched(buffer, fields, client):
    "Only the requested fields were fetched."
    with h5py.File(buffer) as file:
        actual_fields = set(file.keys())
    expected = set(fields or client.keys())  # By default all fields were fetched
    assert actual_fields == expected
