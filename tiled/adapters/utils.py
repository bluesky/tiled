import math
import warnings
from typing import TYPE_CHECKING, Any, Tuple

if TYPE_CHECKING:
    import numpy as np

# for back-compat
from ..utils import node_repr as tree_repr  # noqa: F401

_MESSAGE = (
    "Instead of {name}_indexer[...] use {name}()[...]. "
    "The {name}_indexer accessor is deprecated."
)


def force_reshape(arr: np.array, desired_shape: Tuple[int, ...]) -> np.array:
    """Reshape a numpy array to match the desited shape, if possible.

    Parameters
    ----------

    arr : np.array
        The original ND array to be reshaped
    desired_shape : Tuple[int, ...]
        The desired shape of the resulting array

    Returns
    -------

    A view of the original array
    """

    if arr.shape == desired_shape:
        # Nothing to do here
        return arr

    if arr.size == math.prod(desired_shape):
        if len(arr.shape) != len(desired_shape):
            # Missing or extra singleton dimensions
            warnings.warn(
                f"Forcefully reshaping {arr.shape} to {desired_shape}",
                category=RuntimeWarning,
            )
            return arr.reshape(desired_shape)
        else:
            # Some dimensions might be swapped or completely wrong
            # TODO: needs to be treated more carefully
            pass

    warnings.warn(
        f"Can not reshape array of {arr.shape} to match {desired_shape}; proceeding without changes",
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
