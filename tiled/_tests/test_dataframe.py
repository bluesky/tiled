from urllib.parse import parse_qs, urlparse

import numpy
import pandas.testing
import pytest
from starlette.status import HTTP_400_BAD_REQUEST

from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context, record_history
from ..serialization.table import deserialize_arrow
from ..server.app import build_app
from .utils import URL_LIMITS, fail_with_status_code

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
            pandas.DataFrame({f"column_{i:03d}": i * numpy.ones(5) for i in range(10)}),
            npartitions=1,
        ),
        # a dataframe with mixed types
        "diverse": DataFrameAdapter.from_pandas(
            pandas.DataFrame(
                {
                    "A": numpy.array([1, 2, 3], dtype="|u8"),
                    "B": numpy.array([1, 2, 3], dtype="<f8"),
                    "C": ["one", "two", "three"],
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


def test_reading_diverse_dtypes(context):
    client = from_context(context)
    expected = tree["diverse"].read()
    actual = client["diverse"].read()
    pandas.testing.assert_frame_equal(actual, expected)

    for col in expected.columns:
        actual = client["diverse"][col].read()
        assert numpy.array_equal(expected[col], actual)


def test_dask(context):
    client = from_context(context, "dask")["basic"]
    expected = tree["basic"].read()
    pandas.testing.assert_frame_equal(client.read().compute(), expected)
    pandas.testing.assert_frame_equal(client.compute(), expected)


@pytest.mark.parametrize(
    "url_limit, expected_method",
    (
        (URL_LIMITS.HUGE, "GET"),  # URL query should fit in a GET request
        (URL_LIMITS.DEFAULT, None),  # Expected method is not specified
        (URL_LIMITS.TINY, "POST"),  # URL query is too long for a GET request
    ),
    indirect=["url_limit"],
)
def test_url_limit_bypass(context, url_limit, expected_method):
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
        assert len(requests) == df_client.structure().npartitions

        request_methods = list(request.method for request in requests)
        if expected_method == "POST":
            assert "POST" in request_methods  # At least one POST request
        elif expected_method == "GET":
            assert "POST" not in request_methods  # No POST request


@pytest.mark.parametrize("http_method", ("GET", "POST"))
@pytest.mark.parametrize("link", ("full", "partition"))
def test_http_fetch_columns(context, http_method, link):
    "GET requests beyond the URL length limit should become POST requests."
    if http_method not in ("GET", "POST"):
        pytest.fail(reason="HTTP method {http_method} is not expected.")

    client = from_context(context)
    url_path = client["wide"].item["links"][link]
    original_df = tree["wide"].read()
    columns = list(original_df.columns)[::2]  # Pick a subset of columns
    params = {
        **parse_qs(urlparse(url_path).query),
        "partition": 0,  # Used by /table/partition; ignored by /table/full
        "column": columns,
    }
    expected = tree["wide"].read(columns)
    assert list(expected.columns) == columns

    with record_history() as history:
        if http_method == "POST":
            body = params.pop("column")
            response = context.http_client.post(url_path, json=body, params=params)
        elif http_method == "GET":
            response = context.http_client.get(url_path, params=params)
        response.raise_for_status()
        actual = deserialize_arrow(response.read())
        pandas.testing.assert_frame_equal(actual, expected)
        assert list(actual.columns) == columns

        requests = list(request for request in history.requests)
        assert len(requests) == 1


def test_deprecated_query_parameter(context):
    "HTTP route /table/partition: 'field' is a deprecated query parameter"
    client = from_context(context)
    url_path = client["basic"].item["links"]["partition"]
    params = {
        **parse_qs(urlparse(url_path).query),
        "partition": 0,
        "field": "x",
    }
    with pytest.warns(DeprecationWarning, match=r"'field'"):
        context.http_client.get(url_path, params=params)


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_redundant_query_parameters(context):
    "HTTP route /table/partition accepts 'column' or 'field', but not both"
    client = from_context(context)
    url_path = client["basic"].item["links"]["partition"]
    original_params = {
        **parse_qs(urlparse(url_path).query),
        "partition": 0,
        "field": "x",
        "column": "y",
    }

    # It is OK to include query parameter 'column' OR 'field'
    for param in ("field", "column"):
        params = original_params.copy()
        params.pop(param)
        context.http_client.get(url_path, params=params).raise_for_status()

    # It is an error to include query parameter 'column' AND 'field'
    with fail_with_status_code(HTTP_400_BAD_REQUEST) as response:
        params = original_params
        context.http_client.get(url_path, params=params).raise_for_status()
        assert "'field'" in response.text
        assert "'column'" in response.text
