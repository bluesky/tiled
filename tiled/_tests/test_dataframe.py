from enum import IntEnum

import numpy
import pandas.testing
import pytest

from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context, record_history
from ..client import dataframe as _dataframe_client
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
        # a dataframe with many columns
        "wide": DataFrameAdapter.from_pandas(
            pandas.DataFrame(
                {f"column_{i:03d}": i * numpy.ones(5) for i in range(10)}
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


@pytest.mark.xfail(reason="HTTP API accepts only one column")
def test_dataframe_multicolumn_access(context):
    client = from_context(context)
    original_df = tree["basic"].read()
    columns = list(original_df.columns)
    assert len(columns) > 1
    client["basic"][columns]


def test_dataframe_multicolumn_read(context):
    client = from_context(context)
    original_df = tree["basic"].read()
    columns = list(original_df.columns)[::2]  # Select a subset of columns
    assert len(columns) > 1
    actual_df = client["basic"].read(columns)

    for col in columns:
        expected = original_df[col].values
        actual = actual_df[col]
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


class URL_LIMITS(IntEnum):
    HUGE = 80_000
    ORIGINAL = _dataframe_client.URL_CHARACTER_LIMIT
    TINY = 10


@pytest.fixture
def dataframe_client(request: pytest.FixtureRequest):
    URL_MAX_LENGTH = int(request.param)
    # Temporarily adjust the URL length limit to change client behavior
    _dataframe_client.URL_CHARACTER_LIMIT = URL_MAX_LENGTH
    yield _dataframe_client
    # Then restore the original value
    _dataframe_client.URL_CHARACTER_LIMIT = URL_LIMITS.ORIGINAL


@pytest.mark.parametrize(
    "dataframe_client, expected_method",
    (
        (URL_LIMITS.HUGE, "GET"),  # URL query should fit in a GET request
        (URL_LIMITS.ORIGINAL, None),  # Expected method not specified
        (URL_LIMITS.TINY, "POST"),  # URL query is too long for a GET request
    ),
    indirect=["dataframe_client"],
)
def test_url_limit_bypass(context, dataframe_client, expected_method):
    "GET requests beyond the URL length limit should become POST requests."
    client = from_context(context)
    df_client = client["wide"]
    original_df = tree["wide"].read()
    columns = list(original_df.columns)[::2]  # Pick a subset of columns
    expected = tree["wide"].read(columns)
    assert list(expected.columns) == columns

    with record_history() as history:
        actual = df_client.read(columns)
        pandas.testing.assert_frame_equal(actual, expected)
        assert list(actual.columns) == columns

        requests = list(request for request in history.requests)
        print(f'{requests = }')
        assert len(requests) == df_client.structure().npartitions

        request_methods = list(request.method for request in requests)
        if expected_method == "POST":
            assert "POST" in request_methods  # At least one POST request
        elif expected_method == "GET":
            assert "POST" not in request_methods  # No POST request
