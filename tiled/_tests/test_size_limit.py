import contextlib
import os

import numpy
import pandas
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_config
from ..client.utils import ClientError

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


@contextlib.contextmanager
def fail_with_status_code(status_code):
    with pytest.raises(ClientError) as info:
        yield
    assert info.value.response.status_code == status_code


@contextlib.contextmanager
def low_size_limit():
    os.environ["TILED_RESPONSE_BYTESIZE_LIMIT"] = str(size_limit)
    yield
    os.environ.pop("TILED_RESPONSE_BYTESIZE_LIMIT")


def test_array(tmpdir):
    """
    A password that is wrong, empty, or belonging to a different user fails.
    """

    c = from_config(config)
    path = str(tmpdir / "test.csv")
    c["tiny_array"].read()  # This is fine.
    c["tiny_array"].export(path)  # This is fine.
    with fail_with_status_code(400):
        c["small_array"].read()  # too big
    with fail_with_status_code(400):
        c["small_array"].export(path)  # too big


def test_dataframe(tmpdir):
    """
    A password that is wrong, empty, or belonging to a different user fails.
    """

    c = from_config(config)
    path = str(tmpdir / "test.csv")
    c["tiny_df"].read()  # This is fine.
    c["tiny_df"].export(path)
    with fail_with_status_code(400):
        c["small_df"].read()  # too big
    with fail_with_status_code(400):
        c["small_df"].export(path)  # too big
