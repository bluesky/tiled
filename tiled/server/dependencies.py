from typing import List, Optional, Set

import pydantic_settings
from fastapi import HTTPException, Query, Request
from starlette.status import HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND, HTTP_410_GONE

from tiled.adapters.protocols import AnyAdapter
from tiled.server.schemas import Principal
from tiled.structures.core import StructureFamily

from ..type_aliases import Scopes
from ..utils import BrokenLink
from .core import NoEntry
from .utils import filter_for_access, record_timing


def get_root_tree(request: Request):
    return request.app.state.root_tree


async def get_entry(
    path: str,
    security_scopes: List[str],
    principal: Optional[Principal],
    authn_access_tags: Optional[Set[str]],
    authn_scopes: Scopes,
    root_tree: pydantic_settings.BaseSettings,
    session_state: dict,
    metrics: dict,
    structure_families: Optional[set[StructureFamily]] = None,
    access_policy=None,
) -> AnyAdapter:
    """
    Obtain a node in the tree from its path.

    Walk down the path starting from the root of the tree and filter
    access by the specified scopes.

    session_state is an optional dictionary passed in the session token
    """
    path_parts = [segment for segment in path.split("/") if segment]
    entry = root_tree
    # access_policy = getattr(request.app.state, "access_policy", None)
    # If the entry/adapter can take a session state, pass it in.
    # The entry/adapter may return itself or a different object.
    if hasattr(entry, "with_session_state") and session_state:
        entry = entry.with_session_state(session_state)
    # start at the root
    # filter and keep only what we are allowed to see from here
    entry = await filter_for_access(
        entry,
        access_policy,
        principal,
        authn_access_tags,
        authn_scopes,
        ["read:metadata"],
        metrics,
    )
    try:
        for i, segment in enumerate(path_parts):
            if hasattr(entry, "lookup_adapter"):
                # New catalog adapter
                # This adapter can jump directly to the node of interest,
                # but currenty doesn't, to ensure access_policy is applied.
                # Raises NoEntry or BrokenLink if the path is not found
                entry = await entry.lookup_adapter([segment])
            else:
                # Old-style dict-like interface
                # Traverse into sub-tree(s) to reach the desired entry
                try:
                    entry = entry[segment]
                except (KeyError, TypeError):
                    raise NoEntry(path_parts)

            # filter and keep only what we are allowed to see from here
            entry = await filter_for_access(
                entry,
                access_policy,
                principal,
                authn_access_tags,
                authn_scopes,
                ["read:metadata"],
                metrics,
            )

        # Now check that we have the requested scope according to the access policy
        if access_policy is not None:
            with record_timing(metrics, "acl"):
                allowed_scopes = await access_policy.allowed_scopes(
                    entry,
                    principal,
                    authn_access_tags,
                    authn_scopes,
                )
                if not set(security_scopes).issubset(allowed_scopes):
                    if "read:metadata" not in allowed_scopes:
                        # If you can't read metadata, it does not exist for you.
                        raise NoEntry(path_parts)
                    else:
                        # You can see this, but you cannot perform the requested
                        # operation on it.
                        raise HTTPException(
                            status_code=HTTP_403_FORBIDDEN,
                            detail=(
                                "Not enough permissions to perform this action on this node. "
                                f"Requires scopes {security_scopes}. "
                                f"Principal had scopes {list(allowed_scopes)} on this node."
                            ),
                        )
    except BrokenLink as err:
        raise HTTPException(status_code=HTTP_410_GONE, detail=err.args[0])
    except NoEntry:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND, detail=f"No such entry: {path_parts}"
        )
    # Fast path for the common successful case
    if (structure_families is None) or (entry.structure_family in structure_families):
        return entry
    raise HTTPException(
        status_code=HTTP_404_NOT_FOUND,
        detail=(
            f"The node at {path} has structure family {entry.structure_family} "
            "and this endpoint is compatible with structure families "
            f"{structure_families}"
        ),
    )


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


def shape_param(
    shape: str = Query(..., min_length=1, pattern="^[0-9]+(,[0-9]+)*$|^scalar$"),
):
    "Specify and parse a shape parameter."
    return tuple(map(int, shape.split(",")))


def offset_param(
    offset: str = Query(..., min_length=1, pattern="^[0-9]+(,[0-9]+)*$"),
):
    "Specify and parse an offset parameter."
    return tuple(map(int, offset.split(",")))
