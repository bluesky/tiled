import string
import warnings

import dask.array
import numpy
import pytest

from ..readers.array import ArrayAdapter
from ..client import from_tree
from ..trees.in_memory import Tree


array_cases = {
    "b": (numpy.arange(10) % 2).astype("b"),
    "i": numpy.arange(-10, 10, dtype="i"),
    "uint8": numpy.arange(10, dtype="uint8"),
    "uint16": numpy.arange(10, dtype="uint16"),
    "uint64": numpy.arange(10, dtype="uint64"),
    "f": numpy.arange(10, dtype="f"),
    "c": (numpy.arange(10) * 1j).astype("c"),
    # "m": (
    #     numpy.array(['2007-07-13', '2006-01-13', '2010-08-13'], dtype='datetime64') -
    #     numpy.datetime64('2008-01-01'),
    # )
    # "M": numpy.array(['2007-07-13', '2006-01-13', '2010-08-13'], dtype='datetime64'),
    "S": numpy.array([letter * 3 for letter in string.ascii_letters], dtype="S3"),
    "U": numpy.array([letter * 3 for letter in string.ascii_letters], dtype="U3"),
}
# TODO bitfield "t", void "v", and object "O" (which is not supported by default)
scalar_cases = {k: numpy.array(v[0], dtype=v.dtype) for k, v in array_cases.items()}
for v in scalar_cases.values():
    assert v.shape == ()
array_tree = Tree({k: ArrayAdapter.from_array(v) for k, v in array_cases.items()})
scalar_tree = Tree({k: ArrayAdapter.from_array(v) for k, v in scalar_cases.items()})


@pytest.mark.parametrize("kind", list(array_cases))
def test_array_dtypes(kind):
    client = from_tree(array_tree)
    expected = array_cases[kind]
    actual_via_slice = client[kind][:]
    actual_via_read = client[kind].read()
    assert numpy.array_equal(actual_via_slice, actual_via_read)
    assert numpy.array_equal(actual_via_slice, expected)


@pytest.mark.parametrize("kind", list(scalar_cases))
def test_scalar_dtypes(kind):
    client = from_tree(scalar_tree)
    expected = scalar_cases[kind]
    actual = client[kind].read()
    assert numpy.array_equal(actual, expected)


def test_shape_with_zero():
    expected = numpy.array([]).reshape((0, 100, 1, 10))
    # Suppress RuntimeWarning: divide by zero encountered in true_divide
    # from dask.array.core.
    with warnings.catch_warnings():
        tree = Tree(
            {
                "test": ArrayAdapter(
                    dask.array.from_array(expected, chunks=expected.shape)
                )
            }
        )
    client = from_tree(tree)
    actual = client["test"].read()
    assert numpy.array_equal(actual, expected)
