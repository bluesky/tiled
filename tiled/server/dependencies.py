from functools import lru_cache
from typing import Optional

import pydantic
from fastapi import Depends, HTTPException, Query, Request, Security

from ..media_type_registration import (
    deserialization_registry as default_deserialization_registry,
)
from ..media_type_registration import (
    serialization_registry as default_serialization_registry,
)
from ..query_registration import query_registry as default_query_registry
from ..validation_registration import validation_registry as default_validation_registry
from .authentication import get_current_principal, get_session_state
from .core import NoEntry
from .utils import filter_for_access, record_timing


@lru_cache(1)
def get_query_registry():
    "This may be overridden via dependency_overrides."
    return default_query_registry


@lru_cache(1)
def get_deserialization_registry():
    "This may be overridden via dependency_overrides."
    return default_deserialization_registry


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
    async def inner(
        path: str,
        request: Request,
        principal: str = Depends(get_current_principal),
        root_tree: pydantic.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
    ):
        """
        Obtain a node in the tree from its path.

        Walk down the path from the root tree, filtering each intermediate node by
        'read:metadata' and finally filtering by the specified scope.

        session_state is an optional dictionary passed in the session token
        """
        path_parts = [segment for segment in path.split("/") if segment]
        entry = root_tree

        # If the entry/adapter can take a session state, pass it in.
        # The entry/adapter may return itself or a different object.
        if hasattr(entry, "with_session_state") and session_state:
            entry = entry.with_session_state(session_state)
        try:
            # Traverse into sub-tree(s). This requires only 'read:metadata' scope.
            for i, segment in enumerate(path_parts):
                # add session state to entry
                entry = filter_for_access(
                    entry, principal, ["read:metadata"], request.state.metrics
                )
                # The new catalog adapter only has access control at top level for now.
                # It can jump directly to the node of interest.

                if hasattr(entry, "lookup_adapter"):
                    entry = await entry.lookup_adapter(path_parts[i:])
                    if entry is None:
                        raise NoEntry(path_parts)
                    break
                # Old-style dict-like interface
                else:
                    try:
                        entry = entry[segment]
                    except (KeyError, TypeError):
                        raise NoEntry(path_parts)
            # Now check that we have the requested scope on the final node.
            access_policy = getattr(entry, "access_policy", None)
            if access_policy is not None:
                with record_timing(request.state.metrics, "acl"):
                    allowed_scopes = entry.access_policy.allowed_scopes(
                        entry, principal
                    )
                    if not set(scopes).issubset(allowed_scopes):
                        if "read:metadata" not in allowed_scopes:
                            # If you can't read metadata, it does not exit for you.
                            raise NoEntry(path_parts)
                        else:
                            # You can see this, but you cannot perform the requested
                            # operation on it.
                            raise HTTPException(
                                status_code=403,
                                detail=(
                                    "Not enough permissions to perform this action on this node. "
                                    f"Requires scopes {scopes}. "
                                    f"Principal had scopes {list(allowed_scopes)} on this node."
                                ),
                            )
        except NoEntry:
            raise HTTPException(status_code=404, detail=f"No such entry: {path_parts}")
        return entry

    return Security(inner, scopes=scopes)


def block(
    # Ellipsis as the "default" tells FastAPI to make this parameter required.
    block: str = Query(..., pattern="^[0-9]*(,[0-9]+)*$"),
):
    "Specify and parse a block index parameter."
    if not block:
        return ()
    return tuple(map(int, block.split(",")))


def expected_shape(
    expected_shape: Optional[str] = Query(
        None, min_length=1, pattern="^[0-9]+(,[0-9]+)*$|^scalar$"
    ),
):
    "Specify and parse an expected_shape parameter."
    if expected_shape is None:
        return
    if expected_shape == "scalar":
        return ()
    return tuple(map(int, expected_shape.split(",")))


def slice_(
    slice: str = Query(None, pattern="^[-0-9,:]*$"),
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
