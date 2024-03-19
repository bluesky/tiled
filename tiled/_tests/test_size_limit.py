import contextlib
import os

import numpy
import pandas
import pytest
from starlette.status import HTTP_400_BAD_REQUEST

from ..adapters.array import ArrayAdapter
from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..server.app import build_app_from_config
from .utils import fail_with_status_code

tiny_array = numpy.ones(5)
tiny_df = pandas.DataFrame({"a": tiny_array})
small_array = numpy.ones(50)
small_df = pandas.DataFrame({"a": small_array})
size_limit = small_array.nbytes / 2
assert tiny_array.nbytes < size_limit < small_array.nbytes
assert tiny_df.memory_usage().sum() < size_limit < small_df.memory_usage().sum()


tree = MapAdapter(
    {
        "tiny_array": ArrayAdapter.from_array(tiny_array),
        "small_array": ArrayAdapter.from_array(small_array),
        "tiny_df": DataFrameAdapter.from_pandas(tiny_df, npartitions=1),
        "small_df": DataFrameAdapter.from_pandas(small_df, npartitions=2),
    }
)
config = {
    "trees": [
        {
            "tree": f"{__name__}:tree",
            "path": "/",
        },
    ],
    "response_bytesize_limit": size_limit,
}


@pytest.fixture(scope="module")
def client():
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        client = from_context(context)
        yield client


@contextlib.contextmanager
def low_size_limit():
    os.environ["TILED_RESPONSE_BYTESIZE_LIMIT"] = str(size_limit)
    yield
    os.environ.pop("TILED_RESPONSE_BYTESIZE_LIMIT")


def test_array(client, tmpdir):
    """
    Download an array over the size limit.
    """
    with low_size_limit():
        path = str(tmpdir / "test.csv")
        client["tiny_array"].read()  # This is fine.
        client["tiny_array"].export(path)  # This is fine.
        with fail_with_status_code(HTTP_400_BAD_REQUEST):
            client["small_array"].read()  # too big
        with fail_with_status_code(HTTP_400_BAD_REQUEST):
            client["small_array"].export(path)  # too big


def test_dataframe(client, tmpdir):
    """
    Download an dataframe over the size limit.
    """

    with low_size_limit():
        path = str(tmpdir / "test.csv")
        client["tiny_df"].read()  # This is fine.
        client["tiny_df"].export(path)
        with fail_with_status_code(HTTP_400_BAD_REQUEST):
            client["small_df"].read()  # too big
        with fail_with_status_code(HTTP_400_BAD_REQUEST):
            client["small_df"].export(path)  # too big
