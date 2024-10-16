import math
import warnings
from typing import Any, Optional, Tuple

import numpy as np

# for back-compat
from ..utils import node_repr as tree_repr  # noqa: F401
from .type_alliases import NDSlice

_MESSAGE = (
    "Instead of {name}_indexer[...] use {name}()[...]. "
    "The {name}_indexer accessor is deprecated."
)


def sliced_shape(shp: Tuple[int, ...], slc: Optional[NDSlice] = ...) -> Tuple[int, ...]:
    """Find the shape specification of an array after applying slicing"""

    if slc is Ellipsis:
        return shp
    if isinstance(slc, int):
        return shp[1:]
    if isinstance(slc, slice):
        start, stop, step = slc.indices(shp[0])
        return (
            max(0, (stop - start + (step - (1 if step > 0 else -1))) // step),
            *shp[1:],
        )
    if isinstance(slc, tuple):
        if len(slc) == 0:
            return shp
        else:
            left_axis, *the_rest = slc
            if (left_axis is Ellipsis) and (len(the_rest) < len(shp) - 1):
                the_rest.insert(0, Ellipsis)
            return *sliced_shape(shp[:1], left_axis), *sliced_shape(
                shp[1:], tuple(the_rest)
            )
    return shp


def force_reshape(
    arr: np.array, shp: Tuple[int, ...], slc: Optional[NDSlice] = ...
) -> np.array:
    """Reshape a numpy array to match the desited shape, if possible.

    Returns
    -------

    A view of the original array
    """

    old_shape = arr.shape
    new_shape = sliced_shape(shp, slc)

    if old_shape == new_shape:
        # Nothing to do here
        return arr

    if math.prod(old_shape) == math.prod(new_shape):
        if len(old_shape) != len(new_shape):
            # Missing or extra singleton dimensions
            warnings.warn(
                f"Forcefully reshaping {old_shape} to {new_shape}",
                category=RuntimeWarning,
            )
            return arr.reshape(new_shape)
        else:
            # Some dimensions might be swapped or completely wrong
            # TODO: needs to be treated more carefully
            pass

    warnings.warn(
        f"Can not reshape array of {old_shape} to match {new_shape}; proceeding without changes",
        category=RuntimeWarning,
    )
    return arr


class IndexersMixin:
    """
    Provides slicable attributes keys_indexer, items_indexer, values_indexer.

    This is just for back-ward compatiblity.
    """

    keys: Any
    values: Any
    items: Any
    fn: Any

    @property
    def keys_indexer(self) -> Any:
        """

        Returns
        -------

        """
        warnings.warn(_MESSAGE.format(name="keys"), DeprecationWarning)
        return self.keys()

    @property
    def values_indexer(self) -> Any:
        """

        Returns
        -------

        """
        warnings.warn(_MESSAGE.format(name="values"), DeprecationWarning)
        return self.values()

    @property
    def items_indexer(self) -> Any:
        """

        Returns
        -------

        """
        warnings.warn(_MESSAGE.format(name="items"), DeprecationWarning)
        return self.items()


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
        """

        Parameters
        ----------
        fn :
        """
        self.fn = fn

    def __getitem__(self, key: str) -> Any:
        """

        Parameters
        ----------
        key :

        Returns
        -------

        """
        return self.fn(key)
