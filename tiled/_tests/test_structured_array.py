import numpy as np

import pytest

from tiled.client import from_tree
from tiled.structures.structured_array import StructDtype
from tiled.trees.in_memory import Tree
from tiled.readers.array import StructuredArrayTabularAdapter


@pytest.mark.parametrize(
    "dtype,max_depth",
    [
        (np.dtype("u1, u2"), 1),
        (np.dtype([("a", "u1"), ("b", "f2")]), 1),
        (np.dtype([("a", [("b", "i"), ("c", "f")]), ("d", "c16", (2, 2))]), 2),
        (np.dtype([("a", [("b", "i", (1,)), ("c", "f")]), ("d", "c16", (2, 2))]), 2),
    ],
)
def test_dtype_rount_trip(dtype, max_depth):
    struct = StructDtype.from_numpy_dtype(dtype)
    assert dtype == struct.to_numpy_dtype()
    assert max_depth == struct.max_depth()


def test_fail_subtype():
    with pytest.raises(ValueError):
        StructDtype.from_numpy_dtype(np.dtype("8f"))


def test_read():
    data = np.array(
        [("Rex", 9, 81.0), ("Fido", 3, 27.0)],
        dtype=[("name", "U10"), ("age", "i4"), ("weight", "f4")],
    )
    tree = Tree(
        {
            "A": StructuredArrayTabularAdapter.from_array(data),
        },
    )

    client = from_tree(tree)

    assert np.all(data == client["A"].read())
