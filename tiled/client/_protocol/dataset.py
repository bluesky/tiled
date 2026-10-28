"""Pure planning for assembling an `xarray.Dataset` from a node's children.

The composite and xarray-dataset clients assemble their children into one
`xarray.Dataset`. The decisions involved -- which variables may be batched
into a single wide-table fetch, what role a child plays in the dataset, and
what dimension names each variable gets -- are pure functions of shapes,
specs, and names, shared by the shells. The fetching itself (direct reads,
the wide-table request, progress reporting) stays in the client classes.
"""

from __future__ import annotations

from typing import AbstractSet, Mapping, Optional, Sequence

# Variables longer than this are fetched directly rather than batched into
# a wide table (their size defeats the purpose of batching small fetches).
LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION = 1_000_000


def wide_table_batch(
    shapes: Mapping[str, tuple[int, ...]], enabled: bool = True
) -> set[str]:
    """The names of the variables to fetch together, as one wide table.

    Scalar and short one-dimensional variables of one dataset can be fetched
    in a single request against the container's `full` link instead of one
    request each (see `TableReadRecipe.for_wide_table`). Batching applies
    only when every variable shares the same first-dimension length;
    otherwise the children are ragged, not tabular, and no variable is
    batched.
    """
    first_dims = {shape[0] if shape else None for shape in shapes.values()}
    if not enabled or len(first_dims) > 1:
        return set()
    return {
        name
        for name, shape in shapes.items()
        if (not shape)  # scalar
        or ((shape[0] < LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION) and (len(shape) < 2))
    }


def xarray_variable_role(spec_names: AbstractSet[str]) -> str:
    """Whether a child of an xarray dataset is a "coord" or a "data_var".

    The role is declared by the child's specs, as written by
    `write_xarray_dataset`.
    """
    if "xarray_coord" in spec_names:
        return "coord"
    if "xarray_data_var" in spec_names:
        return "data_var"
    raise ValueError(
        "Child nodes of xarray_dataset should include spec "
        "'xarray_coord' or 'xarray_data_var'."
    )


def assign_dataset_dims(
    shapes: Mapping[str, tuple[int, ...]],
    explicit_dims: Mapping[str, Optional[Sequence[str]]],
    dim0: Optional[str] = None,
) -> dict[str, Sequence[str]]:
    """Dimension names for each variable of an assembled dataset.

    Variables that declare their own dims keep them. When every remaining
    non-scalar variable shares the same first-dimension length, all
    variables share the first dimension's name (`dim0`, by default "dim0")
    and get per-variable names for the other dimensions; otherwise every
    dimension is named per variable, and requesting a shared `dim0` is an error.
    """
    first_dim_lengths = {
        shape[0]
        for name, shape in shapes.items()
        if len(shape) > 0 and not explicit_dims.get(name)
    }
    consistent = len(first_dim_lengths) <= 1
    if dim0 is not None and not consistent:
        raise ValueError(
            "Cannot specify dim0 when the arrays have different left-most dimensions."
        )
    dims = {}
    for name, shape in shapes.items():
        if consistent:
            computed = (dim0 or "dim0",) + tuple(
                f"{name}_dim{i + 1}" for i in range(len(shape) - 1)
            )
        else:
            computed = tuple(f"{name}_dim{i}" for i in range(len(shape)))
        dims[name] = explicit_dims.get(name) or computed
    return dims
