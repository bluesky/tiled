import io
from pathlib import Path

import numpy
import pandas
import pytest
import xarray

from ..adapters.array import ArrayAdapter
from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..adapters.xarray import DatasetAdapter
from ..client import from_tree
from ..client.utils import ClientError

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
                            "time": [1, 2, 3],  # using ints here so HDF5 can export
                        },
                    )
                ),
            }
        ),
    }
)
client = from_tree(tree)


# We test a little bit of actual file export, using the tmpdir fixture,
# but we mostly export for a buffer in memory because disk access
# can be very cloud on cloud CI VMs.


@pytest.mark.parametrize("filename", ["numbers.csv", "image.png", "image.tiff"])
def test_export_2d_array(filename, tmpdir):
    client["A"].export(Path(tmpdir, filename))


@pytest.mark.parametrize("filename", ["numbers.csv", "spreadsheet.xlsx"])
def test_export_table(filename, tmpdir):
    client["C"].export(Path(tmpdir, filename))


def test_export_weather_data_var(tmpdir):
    buffer = io.BytesIO()
    client["structured_data"]["weather"]["temperature"].export(
        buffer, slice=(0,), format="text/csv"
    )


def test_export_weather_all():
    buffer = io.BytesIO()
    client["structured_data"]["weather"].export(buffer, format="application/x-hdf5")


def test_serialization_error_hdf5_metadata():
    good = MapAdapter({}, metadata={"a": 1})
    bad = MapAdapter({}, metadata={"a": {"b": 1}})
    buffer = io.BytesIO()
    from_tree(good).export(buffer, format="application/x-hdf5")
    with pytest.raises(ClientError, match="contains types or structure"):
        from_tree(bad).export(buffer, format="application/x-hdf5")


def test_path_as_Path_or_string(tmpdir):
    client["A"].export(Path(tmpdir, "test_path_as_path.txt"))
    client["A"].export(str(Path(tmpdir, "test_path_as_str.txt")))


def test_formats():
    client.formats
    client["A"].formats
