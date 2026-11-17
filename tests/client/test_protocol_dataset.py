"""Unit tests for the dataset protocol layer (`tiled.client._protocol.dataset`).

These tests exercise the pure planning behind assembling an `xarray.Dataset`
from a node's children, with no server and no I/O.
"""

import pytest

from tiled.client._protocol.dataset import (
    LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION,
    assign_dataset_dims,
    wide_table_batch,
    xarray_variable_role,
)

LONG = LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION


@pytest.mark.parametrize(
    "shapes, enabled, expected",
    [
        # All scalars: everything is batched.
        ({"s1": (), "s2": ()}, True, {"s1", "s2"}),
        # Short 1-D variables are batched; multidimensional ones are
        # fetched directly.
        ({"a": (5,), "b": (5, 3)}, True, {"a"}),
        # Variables at or above the length limit are fetched directly.
        ({"a": (LONG,), "b": (LONG, 2)}, True, set()),
        # Differing first dimensions disable batching entirely (ragged,
        # not tabular) -- a scalar's first dimension counts as distinct.
        ({"a": (5,), "b": (6,)}, True, set()),
        ({"a": (), "b": (5,)}, True, set()),
        # The optimization can be turned off wholesale.
        ({"a": (5,)}, False, set()),
        ({}, True, set()),
    ],
    ids=[
        "scalars",
        "same-first-dim",
        "too-long",
        "ragged",
        "scalar-and-array",
        "disabled",
        "empty",
    ],
)
def test_wide_table_batch(shapes, enabled, expected):
    assert wide_table_batch(shapes, enabled=enabled) == expected


@pytest.mark.parametrize(
    "spec_names, expected",
    [({"xarray_coord"}, "coord"), ({"xarray_data_var", "other"}, "data_var")],
)
def test_xarray_variable_role(spec_names, expected):
    assert xarray_variable_role(spec_names) == expected


def test_xarray_variable_role_requires_spec():
    with pytest.raises(ValueError):
        xarray_variable_role({"unrelated"})


@pytest.mark.parametrize(
    "shapes, explicit_dims, dim0, expected",
    [
        # Consistent first dimensions share the (default) first-dim name.
        (
            {"a": (3,), "b": (3, 2)},
            {},
            None,
            {"a": ("dim0",), "b": ("dim0", "b_dim1")},
        ),
        # The shared first dimension can be named by the caller.
        ({"a": (3,)}, {}, "time", {"a": ("time",)}),
        # Inconsistent first dimensions: every dimension is per-variable.
        (
            {"a": (3,), "b": (4,)},
            {},
            None,
            {"a": ("a_dim0",), "b": ("b_dim0",)},
        ),
        # Explicitly declared dims always win, and do not participate in
        # the consistency check.
        (
            {"a": (3,), "b": (4,)},
            {"b": ("x",)},
            None,
            {"a": ("dim0",), "b": ("x",)},
        ),
    ],
    ids=["consistent", "named-dim0", "inconsistent", "explicit-dims"],
)
def test_assign_dataset_dims(shapes, explicit_dims, dim0, expected):
    assert assign_dataset_dims(shapes, explicit_dims, dim0) == expected


def test_assign_dataset_dims_rejects_dim0_when_inconsistent():
    with pytest.raises(ValueError):
        assign_dataset_dims({"a": (3,), "b": (4,)}, {}, "time")
