import warnings
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Optional

from ..structures.data_source import DataSource

if TYPE_CHECKING:
    from ..type_aliases import AnyStructure

# for back-compat
from ..utils import node_repr as tree_repr  # noqa: F401

_MESSAGE = (
    "Instead of {name}_indexer[...] use {name}()[...]. "
    "The {name}_indexer accessor is deprecated."
)


class IndexersMixin:
    """
    Provides sliceable attributes keys_indexer, items_indexer, values_indexer.

    This is just for back-ward compatibility.
    """

    keys: Any
    values: Any
    items: Any
    fn: Any

    @property
    def keys_indexer(self) -> Any:
        warnings.warn(_MESSAGE.format(name="keys"), DeprecationWarning)
        return self.keys()

    @property
    def values_indexer(self) -> Any:
        warnings.warn(_MESSAGE.format(name="values"), DeprecationWarning)
        return self.values()

    @property
    def items_indexer(self) -> Any:
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
        self.fn = fn

    def __getitem__(self, key: str) -> Any:
        return self.fn(key)


def asset_parameters_to_adapter_kwargs(
    data_source: DataSource["AnyStructure"],
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
    adapter_cls: type[Any],
    data_source: DataSource["AnyStructure"],
    node: Any,  # tiled.catalog.orm.Node ?
    /,
    **kwargs: Optional[Any],
) -> Any:
    # TODO: Sort out typing for Adapters
    """Factory function to produce Adapter instances given their parameters encoded in data sources"""
    parameters = asset_parameters_to_adapter_kwargs(data_source)
    kwargs.update(parameters)
    kwargs["metadata"] = node.metadata_
    kwargs["specs"] = node.specs
    return adapter_cls(structure=data_source.structure, **kwargs)
