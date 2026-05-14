"""Tests for JSON serialization of tables containing numpy/pandas types that
orjson cannot handle natively (numpy scalars, pd.NA, NaT, Timestamp).
"""

import io
import json

import numpy
import pandas
import pytest

from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.client import Context, from_context
from tiled.server.app import build_app_from_config

# DataFrame with every problematic type in one place:
#   - numpy float32 / int32 (orjson rejects non-native numpy scalars)
#   - pandas nullable Int64 with pd.NA
#   - pandas nullable Float64 with pd.NA
#   - datetime column with NaT
#   - pandas Timestamp (object dtype)
_df = pandas.DataFrame(
    {
        "float32_col": numpy.array([1.5, 2.5, float("nan")], dtype=numpy.float32),
        "int32_col": numpy.array([1, 2, 3], dtype=numpy.int32),
        "nullable_int": pandas.array([10, pandas.NA, 30], dtype="Int64"),
        "nullable_float": pandas.array([1.1, pandas.NA, 3.3], dtype="Float64"),
        "datetime_col": pandas.to_datetime(["2024-01-01", "NaT", "2024-03-01"]),
        "timestamp_obj": pandas.array(
            [
                pandas.Timestamp("2024-01-01"),
                pandas.NaT,
                pandas.Timestamp("2024-03-01"),
            ],
            dtype=object,
        ),
    }
)

tree = MapAdapter({"df": DataFrameAdapter.from_pandas(_df, npartitions=1)})

config = {
    "trees": [
        {
            "tree": f"{__name__}:tree",
            "path": "/",
        },
    ],
}


@pytest.fixture(scope="module")
def client():
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        yield from_context(context)


def _read_json(client):
    """Export the df node as application/json and return the parsed dict."""
    buf = io.BytesIO()
    client["df"].export(buf, format="application/json")
    return json.loads(buf.getvalue())


def _read_jsonseq(client):
    """Export the df node as application/json-seq and return list of row dicts."""
    buf = io.BytesIO()
    client["df"].export(buf, format="application/json-seq")
    return [json.loads(line) for line in buf.getvalue().splitlines() if line.strip()]


@pytest.mark.parametrize("reader", [_read_json, _read_jsonseq])
def test_numpy_float32(client, reader):
    """numpy float32 values must round-trip through JSON without TypeError."""
    result = reader(client)
    if isinstance(result, dict):
        col = result["float32_col"]
    else:
        col = [row["float32_col"] for row in result]
    assert abs(col[0] - 1.5) < 1e-4
    assert abs(col[1] - 2.5) < 1e-4
    assert col[2] is None  # NaN → None


@pytest.mark.parametrize("reader", [_read_json, _read_jsonseq])
def test_numpy_int32(client, reader):
    """numpy int32 values must be serialized as plain Python ints."""
    result = reader(client)
    if isinstance(result, dict):
        col = result["int32_col"]
    else:
        col = [row["int32_col"] for row in result]
    assert col == [1, 2, 3]


@pytest.mark.parametrize("reader", [_read_json, _read_jsonseq])
def test_pandas_na_int(client, reader):
    """pandas NA in a nullable integer column must become None in JSON."""
    result = reader(client)
    if isinstance(result, dict):
        col = result["nullable_int"]
    else:
        col = [row["nullable_int"] for row in result]
    assert col[0] == 10
    assert col[1] is None
    assert col[2] == 30


@pytest.mark.parametrize("reader", [_read_json, _read_jsonseq])
def test_pandas_na_float(client, reader):
    """pandas NA in a nullable float column must become None in JSON."""
    result = reader(client)
    if isinstance(result, dict):
        col = result["nullable_float"]
    else:
        col = [row["nullable_float"] for row in result]
    assert abs(col[0] - 1.1) < 1e-6
    assert col[1] is None
    assert abs(col[2] - 3.3) < 1e-6


@pytest.mark.parametrize("reader", [_read_json, _read_jsonseq])
def test_nat_datetime(client, reader):
    """NaT in a datetime column must become None in JSON."""
    result = reader(client)
    if isinstance(result, dict):
        col = result["datetime_col"]
    else:
        col = [row["datetime_col"] for row in result]
    assert col[0] is not None  # valid date → ISO string
    assert col[1] is None  # NaT → None
    assert col[2] is not None


@pytest.mark.parametrize("reader", [_read_json, _read_jsonseq])
def test_timestamp_object(client, reader):
    """pandas Timestamp in object-dtype column must be serialized as ISO string."""
    result = reader(client)
    if isinstance(result, dict):
        col = result["timestamp_obj"]
    else:
        col = [row["timestamp_obj"] for row in result]
    assert isinstance(col[0], str)
    assert col[1] is None  # NaT → None
    assert isinstance(col[2], str)
