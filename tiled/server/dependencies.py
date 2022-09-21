from functools import lru_cache
from typing import Optional

import pydantic
from fastapi import Depends, HTTPException, Query, Request, Security

from ..access_policies import NO_ACCESS
from ..adapters.mapping import MapAdapter
from ..media_type_registration import (
    serialization_registry as default_serialization_registry,
)
from ..query_registration import query_registry as default_query_registry
from ..validation_registration import validation_registry as default_validation_registry
from .authentication import get_current_principal
from .core import NoEntry
from .utils import record_timing

EMPTY_NODE = MapAdapter({})


@lru_cache(1)
def get_query_registry():
    "This may be overridden via dependency_overrides."
    return default_query_registry


@lru_cache(1)
def get_serialization_registry():
    "This may be overridden via dependency_overrides."
    return default_serialization_registry


@lru_cache(1)
def get_validation_registry():
    "This may be overridden via dependency_overrides."
    return default_validation_registry


def get_root_tree():
    raise NotImplementedError(
        "This should be overridden via dependency_overrides. "
        "See tiled.server.app.build_app()."
    )


def SecureEntry(scopes):
    def inner(
        path: str,
        request: Request,
        principal: str = Depends(get_current_principal),
        root_tree: pydantic.BaseSettings = Depends(get_root_tree),
    ):
        """
        Obtain a node in the tree from its path.

        Walk down the path from the root tree, filtering each intermediate node by
        'read:metadata' and finally filtering by the specified scope.
        """
        path_parts = [segment for segment in path.split("/") if segment]
        entry = root_tree
        try:
            # Traverse into sub-tree(s). This requires only 'read:metadata' scope.
            for segment in path_parts[:-1]:
                try:
                    entry = entry[segment]
                except (KeyError, TypeError):
                    raise NoEntry(path_parts)
                access_policy = getattr(entry, "access_policy", None)
                if access_policy is not None:
                    with record_timing(request.state.metrics, "acl"):
                        queries = entry.access_policy.filters(
                            entry, principal, set(["read:metadata"])
                        )
                        if queries is NO_ACCESS:
                            entry = EMPTY_NODE
                        else:
                            for query in queries:
                                entry = entry.search(query)
            # Traverse into last entry. Here we apply the specified scopes.
            if path_parts:
                segment = path_parts[-1]
                try:
                    entry = entry[segment]
                except (KeyError, TypeError):
                    raise NoEntry(path_parts)
                access_policy = getattr(entry, "access_policy", None)
                if access_policy is not None:
                    with record_timing(request.state.metrics, "acl"):
                        queries = entry.access_policy.filters(
                            entry, principal, set(scopes)
                        )
                        if queries is NO_ACCESS:
                            entry = EMPTY_NODE
                        else:
                            for query in queries:
                                entry = entry.search(query)
        except NoEntry:
            raise HTTPException(status_code=404, detail=f"No such entry: {path_parts}")
        return entry

    return Security(inner, scopes=scopes)


def block(
    # Ellipsis as the "default" tells FastAPI to make this parameter required.
    block: str = Query(..., regex="^[0-9]*(,[0-9]+)*$"),
):
    "Specify and parse a block index parameter."
    if not block:
        return ()
    return tuple(map(int, block.split(",")))


def expected_shape(
    expected_shape: Optional[str] = Query(
        None, min_length=1, regex="^[0-9]+(,[0-9]+)*$|^scalar$"
    ),
):
    "Specify and parse an expected_shape parameter."
    if expected_shape is None:
        return
    if expected_shape == "scalar":
        return ()
    return tuple(map(int, expected_shape.split(",")))


def slice_(
    slice: str = Query(None, regex="^[-0-9,:]*$"),
):
    "Specify and parse a block index parameter."
    import numpy

    # IMPORTANT We are eval-ing a user-provider string here so we need to be
    # very careful about locking down what can be in it. The regex above
    # excludes any letters or operators, so it is not possible to execute
    # functions or expensive arithmetic.
    return tuple(
        [
            eval(f"numpy.s_[{dim!s}]", {"numpy": numpy})
            for dim in (slice or "").split(",")
            if dim
        ]
    )
