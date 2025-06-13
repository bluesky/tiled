import csv
import json
from pathlib import Path

import numpy
import pandas
import pytest
import xarray

from ..adapters.array import ArrayAdapter
from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..adapters.xarray import DatasetAdapter
from ..client import Context, from_context
from ..client.utils import ClientError
from ..server.app import build_app

data = numpy.random.random((10, 10))
temp = 15 + 8 * numpy.random.randn(2, 2, 3)
precip = 10 * numpy.random.rand(2, 2, 3)
lon = [[-99.83, -99.32], [-99.79, -99.23]]
lat = [[42.25, 42.21], [42.63, 42.59]]
tree = MapAdapter(
    {
        "A": ArrayAdapter.from_array(numpy.random.random((100, 100))),
        "B": ArrayAdapter.from_array(numpy.random.random((100, 100, 100))),
        "C": DataFrameAdapter.from_pandas(
            pandas.DataFrame(
                {
                    "x": 1 * numpy.random.random(100),
                    "y": 2 * numpy.random.random(100),
                    "z": 3 * numpy.random.random(100),
                }
            ),
            npartitions=3,
        ),
        "empty_table": DataFrameAdapter.from_pandas(
            pandas.DataFrame({"A": []}), npartitions=1
        ),
        "structured_data": MapAdapter(
            {
                "pets": ArrayAdapter.from_array(
                    numpy.array(
                        [("Rex", 9, 81.0), ("Fido", 3, 27.0)],
                        dtype=[("name", "U10"), ("age", "i4"), ("weight", "f4")],
                    )
                ),
                "weather": DatasetAdapter.from_dataset(
                    xarray.Dataset(
                        {
                            "temperature": (["x", "y", "time"], temp),
                            "precipitation": (["x", "y", "time"], precip),
                        },
                        coords={
                            "lon": (["x", "y"], lon),
                            "lat": (["x", "y"], lat),
                            "time": numpy.array(
                                [1, 2, 3]
                            ),  # using ints here so HDF5 can export
                        },
                    )
                ),
            }
        ),
    }
)


@pytest.fixture(scope="module")
def client():
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        yield client


# We test a little bit of actual file export, using the tmpdir fixture,
# but we mostly export for a buffer in memory because disk access
# can be very cloud on cloud CI VMs.


def has_csv_header(filepath):
    with open(filepath, "r") as csv_f:
        sniffer = csv.Sniffer()
        has_header = sniffer.has_header(csv_f.read(2048))
        csv_f.seek(0)
    return has_header


@pytest.mark.parametrize("filename", ["numbers.csv", "image.png", "image.tiff"])
def test_export_2d_array(client, filename, tmpdir):
    client["A"].export(Path(tmpdir, filename))


@pytest.mark.parametrize("filename", ["numbers.csv", "spreadsheet.xlsx"])
def test_export_table(client, filename, tmpdir):
    client["C"].export(Path(tmpdir, filename))


@pytest.mark.parametrize("filename", ["numbers.csv"])
def test_csv_mimetype_opt_params(client, filename, tmpdir):
    client["C"].export(Path(tmpdir, filename), format="text/csv;header=absent")
    assert not has_csv_header(Path(tmpdir, filename))
    client["C"].export(Path(tmpdir, filename), format="text/csv;header=present")
    assert has_csv_header(Path(tmpdir, filename))


def test_streaming_export(client, buffer):
    "The application/json-seq format is streamed via a generator."
    client["C"].export(buffer, format="application/json-seq")
    # Verify that output is valid newline-delimited JSON.
    buffer.seek(0)
    lines = buffer.read().decode().splitlines()
    assert len(lines) == 100
    for line in lines:
        json.loads(line)


def test_streaming_export_empty(client, buffer):
    "The application/json-seq format is streamed via a generator."
    client["empty_table"].export(buffer, format="application/json-seq")
    buffer.seek(0)
    assert buffer.read() == b""


def test_export_weather_data_var(client, tmpdir, buffer):
    client["structured_data"]["weather"]["temperature"].export(
        buffer, slice=(0,), format="text/csv"
    )


def test_export_weather_all(client, buffer):
    client["structured_data"]["weather"].export(buffer, format="application/x-hdf5")


def test_serialization_error_hdf5_metadata(client, buffer):
    tree = MapAdapter(
        {
            "good": MapAdapter({}, metadata={"a": 1}),
            "bad": MapAdapter({}, metadata={"a": {"b": 1}}),
        }
    )
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        client["good"].export(buffer, format="application/x-hdf5")
        with pytest.raises(ClientError, match="contains types or structure"):
            client["bad"].export(buffer, format="application/x-hdf5")


def test_path_as_Path_or_string(client, tmpdir):
    client["A"].export(Path(tmpdir, "test_path_as_path.txt"))
    client["A"].export(str(Path(tmpdir, "test_path_as_str.txt")))


def test_formats(client):
    client.formats
    client["A"].formats
    client["C"].formats
