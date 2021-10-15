from pathlib import Path

import dask.array
import numpy
import pandas
import pytest
import xarray

from ..readers.array import ArrayAdapter, StructuredArrayTabularAdapter
from ..readers.dataframe import DataFrameAdapter
from ..readers.xarray import DataArrayAdapter, DatasetAdapter, VariableAdapter
from ..client import from_tree
from ..trees.in_memory import Tree


data = numpy.random.random((10, 10))
tree = Tree(
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
        "structured_data": Tree(
            {
                "pets": StructuredArrayTabularAdapter.from_array(
                    numpy.array(
                        [("Rex", 9, 81.0), ("Fido", 3, 27.0)],
                        dtype=[("name", "U10"), ("age", "i4"), ("weight", "f4")],
                    )
                ),
                "xarray_variable": VariableAdapter(
                    xarray.Variable(
                        data=dask.array.from_array(data),
                        dims=["x", "y"],
                        attrs={"thing": "stuff"},
                    )
                ),
                "image_with_coords": DataArrayAdapter(
                    xarray.DataArray(
                        xarray.Variable(
                            data=dask.array.from_array(data),
                            dims=["x", "y"],
                            attrs={"thing": "stuff"},
                        ),
                        coords={
                            "x": dask.array.arange(len(data)) / 10,
                            "y": dask.array.arange(len(data)) / 50,
                        },
                    ),
                ),
                "xarray_dataset": DatasetAdapter(
                    xarray.Dataset(
                        {
                            "image": xarray.DataArray(
                                xarray.Variable(
                                    data=dask.array.from_array(data),
                                    dims=["x", "y"],
                                    attrs={"thing": "stuff"},
                                ),
                                coords={
                                    "x": dask.array.arange(len(data)) / 10,
                                    "y": dask.array.arange(len(data)) / 50,
                                },
                            ),
                            "z": xarray.DataArray(data=dask.array.ones((len(data),))),
                        }
                    )
                ),
                "xarray_data_array": DataArrayAdapter(
                    xarray.DataArray(
                        xarray.Variable(
                            data=dask.array.from_array(data),
                            dims=["x", "y"],
                            attrs={"thing": "stuff"},
                        )
                    )
                ),
            }
        ),
    },
)


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
@pytest.mark.parametrize("filename", ["numbers.csv", "image.png", "image.tiff"])
def test_export_2d_array(filename, structure_clients, tmpdir):
    client = from_tree(tree, structure_clients=structure_clients)
    client["A"].export(Path(tmpdir, filename))


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
@pytest.mark.parametrize("filename", ["numbers.csv", "spreadsheet.xlsx"])
def test_export_table(filename, structure_clients, tmpdir):
    client = from_tree(tree, structure_clients=structure_clients)
    client["C"].export(Path(tmpdir, filename))


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
@pytest.mark.parametrize("filename", ["numbers.csv"])
def test_export_xarray_variable(filename, structure_clients, tmpdir):
    client = from_tree(tree, structure_clients=structure_clients)
    client["structured_data"]["xarray_variable"].export(Path(tmpdir, filename))


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
@pytest.mark.parametrize("filename", ["numbers.csv"])
def test_export_xarray_data_array(filename, structure_clients, tmpdir):
    client = from_tree(tree, structure_clients=structure_clients)
    client["structured_data"]["xarray_data_array"].export_array(Path(tmpdir, filename))


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
@pytest.mark.parametrize("filename", ["numbers.csv"])
def test_export_xarray_data_array_coord(filename, structure_clients, tmpdir):
    client = from_tree(tree, structure_clients=structure_clients)
    client["structured_data"]["image_with_coords"].coords["x"].export_array(
        Path(tmpdir, filename)
    )


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
@pytest.mark.parametrize("filename", ["numbers.csv"])
def test_export_xarray_dataset_data_var(filename, structure_clients, tmpdir):
    client = from_tree(tree, structure_clients=structure_clients)
    client["structured_data"]["xarray_dataset"].data_vars["image"].export_array(
        Path(tmpdir, filename)
    )


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
@pytest.mark.parametrize("filename", ["numbers.csv"])
def test_export_xarray_dataset_coord(filename, structure_clients, tmpdir):
    client = from_tree(tree, structure_clients=structure_clients)
    client["structured_data"]["xarray_dataset"].coords["x"].export_array(
        Path(tmpdir, filename)
    )


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
@pytest.mark.parametrize("filename", ["test.nc"])
def test_export_xarray_dataset_all(filename, structure_clients, tmpdir):
    client = from_tree(tree, structure_clients=structure_clients)
    client["structured_data"]["xarray_dataset"].export(Path(tmpdir, filename))


def test_path_as_Path_or_string(tmpdir):
    client = from_tree(tree)
    client["A"].export(Path(tmpdir, "test_path_as_path.txt"))
    client["A"].export(str(Path(tmpdir, "test_path_as_str.txt")))
