import math
import string
import warnings
from pathlib import Path

import dask.array
import httpx
import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_tree

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
array_tree = MapAdapter({k: ArrayAdapter.from_array(v) for k, v in array_cases.items()})
scalar_tree = MapAdapter(
    {k: ArrayAdapter.from_array(v) for k, v in scalar_cases.items()}
)

cube_cases = {
    "tiny_cube": numpy.random.random((10, 10, 10)),
    "tiny_hypercube": numpy.random.random((10, 10, 10, 10, 10)),
}
cube_tree = MapAdapter({k: ArrayAdapter.from_array(v) for k, v in cube_cases.items()})


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
        tree = MapAdapter(
            {
                "test": ArrayAdapter(
                    dask.array.from_array(expected, chunks=expected.shape)
                )
            }
        )
    client = from_tree(tree)
    actual = client["test"].read()
    assert numpy.array_equal(actual, expected)


def test_nan_infinity_handler(tmpdir):
    data = numpy.array([0, 1, numpy.NAN, -numpy.Inf, numpy.Inf])
    metadata = {"infinity": math.inf, "-infinity": -math.inf, "nan": numpy.NAN}
    inf_tree = MapAdapter(
        {"example": ArrayAdapter.from_array(data, metadata=metadata)}, metadata=metadata
    )

    client = from_tree(inf_tree)
    print(f"Metadata: {client['example'].metadata}")
    print(f"Data: {client['example'].read()}")
    Path(tmpdir, "testjson").mkdir()
    client["example"].export(Path(tmpdir, "testjson", "test.json"))

    import json

    def strict_parse_constant(c):
        raise ValueError(f"{c} is not valid JSON")

    open_json = json.load(
        open(Path(tmpdir, "testjson", "test.json"), "r"),
        parse_constant=strict_parse_constant,
    )

    expected_list = [0.0, 1.0, None, None, None]
    assert open_json == expected_list


def test_array_format_shape_from_cube():
    client = from_tree(cube_tree)

    with pytest.raises(httpx.HTTPStatusError) as err:
        # export...
        hyper_cube = client["tiny_hypercube"].export("test.png")  # noqa: F841
    # Check that the error is 406 (Not Acceptable).
    assert err.match("406")


def test_array_interface():
    client = from_tree(array_tree)
    for k, v in client.items():
        assert v.shape == array_cases[k].shape
        assert v.ndim == array_cases[k].ndim
        assert v.nbytes == array_cases[k].nbytes
        assert v.dtype == array_cases[k].dtype
        assert numpy.array_equal(numpy.asarray(v), array_cases[k])
        # smoke test
        v.chunks
        v.dims
