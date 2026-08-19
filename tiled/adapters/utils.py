import math
from collections import defaultdict
from typing import Any, Optional, Tuple, Union

import dask.array
import numpy as np

from tiled.adapters.core import A, S

from ..structures.data_source import DataSource

# for back-compat
from ..utils import IndexersMixin  # noqa: F401
from ..utils import node_repr as tree_repr  # noqa: F401

__all__ = [
    "IndexersMixin",
    "asset_parameters_to_adapter_kwargs",
    "init_adapter_from_catalog",
]


class IndexCallable:
    """
    DEPRECATED and no longer used internally

    Provide getitem syntax for functions

    >>> def inc(x):
    ...     return x + 1

    >>> I = IndexCallable(inc)
    >>> I[3]
    4

    Vendored from dask
    """

    __slots__ = ("fn",)

    def __init__(self, fn: Any) -> None:
        self.fn = fn

    def __getitem__(self, key: str) -> Any:
        return self.fn(key)


def asset_parameters_to_adapter_kwargs(
    data_source: DataSource[Any],
) -> dict[str, Any]:
    """Transform database representation of Adapter parameters to Python representation."""
    parameters: dict[str, Any] = defaultdict(list)
    for asset in data_source.assets:
        if (asset.num is not None) or (asset.parameter == "data_uris"):
            # This asset is associated with a parameter that takes a list of URIs.
            param = asset.parameter or "data_uris"
            parameters[param].append(asset.data_uri)
        else:
            # This asset is associated with a parameter that takes a single URI.
            param = asset.parameter or "data_uri"
            parameters[param] = asset.data_uri

    return parameters


def init_adapter_from_catalog(
    adapter_cls: type[A],
    data_source: DataSource[S],
    node: Any,  # tiled.catalog.orm.Node ?
    /,
    **kwargs: Optional[Any],
) -> A:
    # TODO: Sort out typing for Adapters
    """Factory function to produce Adapter instances given their parameters encoded in data sources"""
    parameters = asset_parameters_to_adapter_kwargs(data_source)
    kwargs.update(parameters)
    kwargs["metadata"] = node.metadata_
    kwargs["specs"] = node.specs
    return adapter_cls(structure=data_source.structure, **kwargs)


def force_reshape(
    arr: Union[np.array, dask.array.Array], desired_shape: Tuple[int, ...]
) -> Union[np.array, dask.array.Array]:
    """Reshape a numpy or dask array to match the desired shape, if possible.

    Parameters
    ----------

    arr : Union[np.array, dask.array.Array]
        The original ND array to be reshaped
    desired_shape : Tuple[int, ...]
        The desired shape of the resulting array

    Returns
    -------

    A view of the original array
    """

    if arr.shape == tuple(desired_shape):
        return arr  # Nothing to do here

    if arr.size == math.prod(desired_shape):
        return arr.reshape(desired_shape)

    raise ValueError(
        f"Can not reshape {arr.shape} array data to {tuple(desired_shape)}"
    )


def split_chunks(total: int, chunk: int) -> tuple[int, ...]:
    "Split total into repeated chunks of size `chunk`, with a remainder at the end."
    num_full_chunks, remainder = divmod(total, chunk)
    return tuple([chunk] * num_full_chunks + ([remainder] if remainder else []))


def grid_shape_for_files(
    struct_shape: Tuple[int, ...], n_files: int
) -> Optional[Tuple[int, ...]]:
    """For multi-asset datasets, return the leading axes of `struct_shape` that
    enumerate the files, one grid cell per file, or `None` when no clean split exists.

    Context: `n_files` files were each read as one fixed-shape frame, stacked
    into a new leading axis of length `n_files`, and that axis was then reshaped
    into one or more leading dimensions of `struct_shape` (the trailing
    dimensions are the shared per-frame shape). This recovers the grid of those
    leading dimensions so a caller can map a grid cell back to its one file.

    The split is clean only when a prefix of `struct_shape` multiplies to exactly
    `n_files`: the smallest `j` with `prod(struct_shape[:j]) == n_files`. Then the
    leading `j` dimensions index the files and this returns `struct_shape[:j]`.

    For example, 12 files reshaped to `(3, 4, H, W)` returns `(3, 4)` -- the file
    at grid cell `(i, j)` supplies the `H x W` frame at `[i, j]`. But `(5, ...)`
    with 12 files returns `None`: no prefix hits 12, so a single file's frame
    would straddle a dimension boundary and the leading indices no longer
    correspond one-to-one with files.

    The running product only grows (every dimension is `>= 1`), so once it passes
    `n_files` without landing on it, no aligned split can exist and this stops.
    """
    product = 1
    for j, dim in enumerate(struct_shape, start=1):
        product *= dim
        if product == n_files:
            return struct_shape[:j]
        if product > n_files:
            break
    return None
