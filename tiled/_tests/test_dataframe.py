import numpy
import pandas.testing
import pytest

from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..server.app import build_app

tree = MapAdapter(
    {
        # a dataframe divided into three partitions
        "basic": DataFrameAdapter.from_pandas(
            pandas.DataFrame(
                {
                    "x": 1 * numpy.ones(10),
                    "y": 2 * numpy.ones(10),
                    "z": 3 * numpy.ones(10),
                }
            ),
            npartitions=3,
        ),
        # a dataframe with just one partition
        "single_partition": DataFrameAdapter.from_pandas(
            pandas.DataFrame(
                {
                    "x": 1 * numpy.ones(5),
                    "y": 2 * numpy.ones(5),
                    "z": 3 * numpy.ones(5),
                }
            ),
            npartitions=1,
        ),
    }
)


@pytest.fixture(scope="module")
def context():
    app = build_app(tree)
    with Context.from_app(app) as context:
        yield context


def test_dataframe_basic(context):
    client = from_context(context)
    expected = tree["basic"].read()
    actual = client["basic"].read()
    assert client["basic"].structure().npartitions == 3
    pandas.testing.assert_frame_equal(actual, expected)
    assert client["basic"].columns == list(expected.columns) == list(actual.columns)


def test_dataframe_column_access(context):
    client = from_context(context)
    expected_df = tree["basic"].read()
    for col in expected_df.columns:
        expected = expected_df[col].values
        actual = client["basic"][col]
        numpy.testing.assert_equal(actual, expected)


def test_dataframe_single_partition(context):
    client = from_context(context)
    expected = tree["single_partition"].read()
    actual = client["single_partition"].read()
    assert client["single_partition"].structure().npartitions == 1
    pandas.testing.assert_frame_equal(actual, expected)


def test_dask(context):
    client = from_context(context, "dask")["basic"]
    expected = tree["basic"].read()
    pandas.testing.assert_frame_equal(client.read().compute(), expected)
    pandas.testing.assert_frame_equal(client.compute(), expected)
