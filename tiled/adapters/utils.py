from collections import defaultdict
from typing import Any, Optional

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
