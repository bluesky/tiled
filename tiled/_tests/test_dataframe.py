import numpy
import pandas.testing

from ..readers.dataframe import DataFrameAdapter
from ..client import from_tree
from ..trees.in_memory import Tree


tree = Tree(
    {
        # a dataframe divided into three partitions
        "basic": DataFrameAdapter.from_pandas(
            pandas.DataFrame(
                {
                    "x": 1 * numpy.ones(100),
                    "y": 2 * numpy.ones(100),
                    "z": 3 * numpy.ones(100),
                }
            ),
            npartitions=3,
        ),
        # a dataframe with just one partition
        "single_partition": DataFrameAdapter.from_pandas(
            pandas.DataFrame(
                {
                    "x": 1 * numpy.ones(100),
                    "y": 2 * numpy.ones(100),
                    "z": 3 * numpy.ones(100),
                }
            ),
            npartitions=1,
        ),
    }
)


def test_dataframe_basic():
    client = from_tree(tree)
    expected = tree["basic"].read()
    actual = client["basic"].read()
    assert client["basic"].structure().macro.npartitions == 3
    pandas.testing.assert_frame_equal(actual, expected)


def test_dataframe_column_access():
    client = from_tree(tree)
    expected_df = tree["basic"].read()
    for col in expected_df.columns:
        expected = expected_df[col]
        actual = client["basic"][col]
        pandas.testing.assert_series_equal(actual, expected)


def test_dataframe_single_partition():
    client = from_tree(tree)
    expected = tree["single_partition"].read()
    actual = client["single_partition"].read()
    assert client["single_partition"].structure().macro.npartitions == 1
    pandas.testing.assert_frame_equal(actual, expected)
