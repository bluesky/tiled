import builtins
import collections
import re
from functools import lru_cache
from typing import Optional

import pydantic
from fastapi import Depends, HTTPException, Query, Request, Security

from ..media_type_registration import (
    serialization_registry as default_serialization_registry,
)
from ..query_registration import query_registry as default_query_registry
from ..validation_registration import validation_registry as default_validation_registry
from .authentication import get_current_principal
from .core import NoEntry
from .utils import filter_for_access, record_timing


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
            for segment in path_parts:
                entry = filter_for_access(
                    entry, principal, ["read:metadata"], request.state.metrics
                )
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


# Accept numpy-style mutlidimesional slices and special constructs 'a:b:mean'
# and 'a:b:mean(c)' to represent downsampling, inspired by
# https://uhi.readthedocs.io/en/latest/
# Note: if you need to debug this, the interactive tool at https://regex101.com/ is your friend!
DIM_REGEX = r"(?:(-?[0-9]+)|(?:([0-9]*|-[0-9]+):(?:([0-9]*|-[0-9]+))?(?::(mean|mean\([0-9]+\)|[0-9]*|-[0-9]+))?))"
SLICE_REGEX = rf"^{DIM_REGEX}(,{DIM_REGEX})*$"
DIM_PATTERN = re.compile(rf"^{DIM_REGEX}$")
MEAN_PATTERN = re.compile(r"(mean|mean\(([0-9]+)\))")


# This object is meant to be placed at slice.step and used by the consumer to
# detect that it should agggregate, using
# numpy.mean or skimage.transform.downscale_local_mean.
Mean = collections.namedtuple("Mean", ["parameter"])


def _int_or_none(s):
    return int(s) if s else None


def _mean_int_or_none(s):
    if s is None:
        return None
    m = MEAN_PATTERN.match(s)
    if m.group(0):
        return Mean(m.group(1))
    return _int_or_none(s)


def slice_(
    slice: str = Query("", regex=SLICE_REGEX),
):
    "Specify and parse a slice parameter."
    slices = []
    for dim in slice.split(","):
        if dim:
            match = DIM_PATTERN.match(dim)
            # Group 1 matches an int, as in arr[i].
            if match.group(1) is not None:
                s = int(match.group(1))
            else:
                # Groups 2 through 4 match a slice, as in arr[i:j:k].
                s = builtins.slice(
                    _int_or_none(match.group(2)),
                    _int_or_none(match.group(3)),
                    _mean_int_or_none(match.group(4)),
                )
            slices.append(s)
    print("slices", slices)
    return tuple(slices)
