from typing import Optional, Union

import pydantic_settings
from fastapi import Depends, HTTPException, Query, Request, Security
from fastapi.security import SecurityScopes
from starlette.status import HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND, HTTP_410_GONE

from tiled.adapters.mapping import MapAdapter
from tiled.server.schemas import Principal
from tiled.structures.core import StructureFamily
from tiled.utils import SpecialUsers

from ..utils import BrokenLink
from .authentication import check_scopes, get_current_principal, get_session_state
from .core import NoEntry
from .utils import filter_for_access, record_timing


def get_root_tree():
    raise NotImplementedError(
        "This should be overridden via dependency_overrides. "
        "See tiled.server.app.build_app()."
    )


def get_entry(structure_families: Optional[set[StructureFamily]] = None):
    async def inner(
        path: str,
        request: Request,
        security_scopes: SecurityScopes,
        principal: Union[Principal, SpecialUsers] = Depends(get_current_principal),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes),
    ) -> MapAdapter:
        """
        Obtain a node in the tree from its path.

        Walk down the path from the root tree, discover the access policy
        to be used for access to the destination node, and finally filter
        access by the specified scope.

        The access policy used for access to the destination node will be
        the last one found while walking the tree or, in the case of a catalog adapter,
        the access policy of the catalog adapter node.

        session_state is an optional dictionary passed in the session token
        """
        path_parts = [segment for segment in path.split("/") if segment]
        path_parts_relative = path_parts
        entry = root_tree
        entry_with_access_policy = (
            entry if getattr(root_tree, "access_policy", None) is not None else None
        )

        # If the entry/adapter can take a session state, pass it in.
        # The entry/adapter may return itself or a different object.
        if hasattr(entry, "with_session_state") and session_state:
            entry = entry.with_session_state(session_state)
        # start at the root
        # filter and keep only what we are allowed to see from here
        entry = await filter_for_access(
            entry,
            principal,
            ["read:metadata"],
            request.state.metrics,
            path_parts_relative,
        )
        try:
            for i, segment in enumerate(path_parts):
                if hasattr(entry, "lookup_adapter"):
                    # New catalog adapter - only has access control at the top level
                    # Top level means the basename of the path as defined in the config
                    # This adapter can jump directly to the node of interest
                    # Raises NoEntry or BrokenLink if the path is not found
                    entry = await entry.lookup_adapter(path_parts[i:])
                    break
                else:
                    # Old-style dict-like interface
                    # Traverse into sub-tree(s) to reach the desired entry, and
                    # to discover the access policy to use for the request
                    try:
                        entry = entry[segment]
                    except (KeyError, TypeError):
                        raise NoEntry(path_parts)
                    if getattr(entry, "access_policy", None) is not None:
                        path_parts_relative = path_parts[i + 1 :]  # noqa: E203
                        entry_with_access_policy = entry
                        # filter and keep only what we are allowed to see from here
                        entry = await filter_for_access(
                            entry,
                            principal,
                            ["read:metadata"],
                            request.state.metrics,
                            path_parts_relative,
                        )

            # Now check that we have the requested scope according to the discovered access policy
            access_policy = getattr(entry_with_access_policy, "access_policy", None)
            if access_policy is not None:
                with record_timing(request.state.metrics, "acl"):
                    allowed_scopes = await access_policy.allowed_scopes(
                        entry_with_access_policy, principal, path_parts_relative
                    )
                    if not set(security_scopes.scopes).issubset(allowed_scopes):
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
                                    f"Requires scopes {security_scopes.scopes}. "
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
        if (structure_families is None) or (
            entry.structure_family in structure_families
        ):
            return entry
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=(
                f"The node at {path} has structure family {entry.structure_family} "
                "and this endpoint is compatible with structure families "
                f"{structure_families}"
            ),
        )

    return inner


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
