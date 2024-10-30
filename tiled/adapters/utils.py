import warnings
from typing import Any

# for back-compat
from ..utils import node_repr as tree_repr  # noqa: F401

_MESSAGE = (
    "Instead of {name}_indexer[...] use {name}()[...]. "
    "The {name}_indexer accessor is deprecated."
)


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
