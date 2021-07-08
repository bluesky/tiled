import string

from httpx import AsyncClient
import numpy
import pandas
import pytest

from ..readers.array import ArrayAdapter
from ..client import from_client
from ..readers.dataframe import DataFrameAdapter
from ..structures.array import Kind
from ..trees.in_memory import Tree
from ..server.app import serve_tree


cases = {
    "b": (numpy.arange(10) % 2).astype("b"),
    "i": numpy.arange(-10, 10),
    # "i": numpy.arange(-10, 10, dtype="i"),
    "uint8": numpy.arange(10, dtype="uint8"),
    "uint16": numpy.arange(10, dtype="uint16"),
    "uint64": numpy.arange(10, dtype="uint64"),
    "f": numpy.arange(10, dtype="f"),
    "c": (numpy.arange(10) * 1j).astype("c"),
    "m": numpy.array(['2007-07-13', '2006-01-13', '2010-08-13'], dtype='datetime64') - numpy.datetime64('2008-01-01'),
    "M": numpy.array(['2007-07-13', '2006-01-13', '2010-08-13'], dtype='datetime64'),
    "S": numpy.array([letter * 3 for letter in string.ascii_letters], dtype='S3'),
    "U": numpy.array([letter * 3 for letter in string.ascii_letters], dtype='U3'),
}
# TODO bitfield "t", void "v", and object "O" (which is not supported by default)
tree = Tree(
    {k: ArrayAdapter.from_array(v) for k, v in cases.items()}
)



@pytest.mark.parametrize("kind", list(cases))
@pytest.mark.asyncio
async def test_array_dtypes(kind):
    app = serve_tree(tree, authentication={"allow_anonymous_access": True})
    expected = cases[kind]
    async with AsyncClient(app=app, base_url="http://test") as ac:
        client = from_client(ac)
        actual = client[kind]
    assert numpy.array_equal(actual, expected)
