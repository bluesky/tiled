from pathlib import Path

import numpy
import pandas
import pytest

from ..readers.array import ArrayAdapter
from ..readers.dataframe import DataFrameAdapter
from ..client import from_tree
from ..trees.in_memory import Tree


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


def test_path_as_Path_or_string(tmpdir):
    client = from_tree(tree)
    client["A"].export(Path(tmpdir, "test_path_as_path.txt"))
    client["A"].export(str(Path(tmpdir, "test_path_as_str.txt")))
