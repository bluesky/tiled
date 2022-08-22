from functools import lru_cache
from typing import Optional

import pydantic
from fastapi import Depends, HTTPException, Query, Request

from ..media_type_registration import (
    serialization_registry as default_serialization_registry,
)
from ..query_registration import query_registry as default_query_registry
from ..validation_registration import validation_registry as default_validation_registry
from .authentication import get_current_principal
from .core import NoEntry
from .utils import record_timing


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


def entry(
    path: str,
    request: Request,
    principal: str = Depends(get_current_principal),
    root_tree: pydantic.BaseSettings = Depends(get_root_tree),
):
    path_parts = [segment for segment in path.split("/") if segment]
    entry = root_tree.authenticated_as(principal)
    try:
        # Traverse into sub-tree(s).
        for segment in path_parts:
            try:
                unauthenticated_entry = entry[segment]
            except (KeyError, TypeError):
                raise NoEntry(path_parts)
            if hasattr(unauthenticated_entry, "authenticated_as"):
                with record_timing(request.state.metrics, "acl"):
                    entry = unauthenticated_entry.authenticated_as(principal)
            else:
                entry = unauthenticated_entry
        return entry
    except NoEntry:
        raise HTTPException(status_code=404, detail=f"No such entry: {path_parts}")


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
