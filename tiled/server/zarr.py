import dataclasses
import inspect
import json
import os
import re
import warnings
from datetime import datetime, timedelta
from functools import partial, wraps
from pathlib import Path
from typing import Any, List, Optional, Tuple

import anyio
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Security
from jmespath.exceptions import JMESPathError
from json_merge_patch import merge as apply_merge_patch
from jsonpatch import apply_patch as apply_json_patch
from pydantic_settings import BaseSettings
from starlette.responses import Response
from starlette.status import (
    HTTP_200_OK,
    HTTP_206_PARTIAL_CONTENT,
    HTTP_400_BAD_REQUEST,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_405_METHOD_NOT_ALLOWED,
    HTTP_406_NOT_ACCEPTABLE,
    HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
    HTTP_422_UNPROCESSABLE_ENTITY,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from .. import __version__
from ..structures.core import Spec, StructureFamily
from ..structures.array import StructDtype
from ..utils import ensure_awaitable, patch_mimetypes, path_from_uri
from ..validation_registration import ValidationError

# from . import schemas
from .authentication import Mode, get_authenticators, get_current_principal
from .core import (
    DEFAULT_PAGE_SIZE,
    DEPTH_LIMIT,
    MAX_PAGE_SIZE,
    NoEntry,
    UnsupportedMediaTypes,
    WrongTypeForRoute,
    apply_search,
    construct_data_response,
    construct_entries_response,
    construct_resource,
    construct_revisions_response,
    json_or_msgpack,
    resolve_media_type,
)
from .dependencies import (
    SecureEntry,
    block,
    expected_shape,
    get_deserialization_registry,
    get_query_registry,
    get_serialization_registry,
    get_validation_registry,
    slice_,
)
from .file_response_with_range import FileResponseWithRange
from .links import links_for_node
from .settings import get_settings
from .utils import filter_for_access, get_base_url, record_timing

ZARR_BLOCK_SIZE = 10000
ZARR_BYTE_ORDER = "C"
ZARR_CODEC_SPEC = {
    "blocksize": 0,
    "clevel": 5,
    "cname": "lz4",
    "id": "blosc",
    "shuffle": 1,
}
ZARR_DATETIME64_PRECISION = 'ns'

import numcodecs

zarr_codec = numcodecs.get_codec(ZARR_CODEC_SPEC)

router = APIRouter()


def convert_chunks_for_zarr(tiled_chunks: Tuple[Tuple[int]]):
    """Convert full tiled/dask chunk specification into zarr format

    Zarr only accepts chunks of constant size along each dimension; this function finds a unique representation of
    (possibly variable-sized chunks) internal to Tiled ArrayAdapter in terms of zarr blocks.

    Zarr chunks must be at least of size 1 (even for zero-dimensional arrays).
    """
    return [min(ZARR_BLOCK_SIZE, max(*tc, 1)) for tc in tiled_chunks]


@router.get("{path:path}.zgroup", name="Root .zgroup metadata")
@router.get("/{path:path}/.zgroup", name="Zarr .zgroup metadata")
async def get_zarr_group_metadata(
    request: Request,
    entry=SecureEntry(
        scopes=["read:data", "read:metadata"],
        structure_families={StructureFamily.table, StructureFamily.container, StructureFamily.array},
    ),
):
    # Usual (unstructured) array; should respond to /.zarray instead
    if entry.structure_family == StructureFamily.array and not isinstance(entry.structure().data_type, StructDtype):
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)

    # Structured numpy array, Container, or Table
    return Response(json.dumps({"zarr_format": 2}), status_code=200)

@router.get("/{path:path}/.zarray", name="Zarr .zarray metadata")
async def get_zarr_array_metadata(
    request: Request,
    entry=SecureEntry(
        scopes=["read:data", "read:metadata"],
        structure_families={StructureFamily.array, StructureFamily.sparse},
    ),
):
    # Only StructureFamily.array and StructureFamily.sparse can respond to `/.zarray` querries. Zarr will try to
    # request .zarray on all other nodes in Tiled (not included in SecureEntry above), in which case the server
    # will return an 404 error; this is the expected behaviour, which will signal zarr to try /.zgroup instead.
    structure = entry.structure()
    if isinstance(structure.data_type, StructDtype):
        # Structured numpy array should be treated as a DataFrame and will respond to /.zgroup instead
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    try:
        zarray_spec = {
            "chunks": convert_chunks_for_zarr(structure.chunks),
            "compressor": ZARR_CODEC_SPEC,
            "dtype": structure.data_type.to_numpy_str(),
            "fill_value": 0,
            "filters": None,
            "order": ZARR_BYTE_ORDER,
            "shape": list(structure.shape),
            "zarr_format": 2,
        }
        return Response(json.dumps(zarray_spec), status_code=200)
    except Exception as err:
        print(f"Can not create .zarray metadata, {err}")
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR, detail=err.args[0]
        )


@router.get(
    "/{path:path}", name="Zarr group (directory) structure or a chunk of a zarr array"
)
async def get_zarr_array(
    request: Request,
    block: str | None = None,
    entry=SecureEntry(
        scopes=["read:data"],
        structure_families={
            StructureFamily.array,
            StructureFamily.sparse,
            StructureFamily.table,
            StructureFamily.container,
        },
    ),
):
    # Remove query params and the trailing slash from the url
    url = str(request.url).split("?")[0].rstrip("/")

    if entry.structure_family == StructureFamily.container:
        # List the contents of a "simulated" zarr directory (excluding .zarray and .zgroup files)
        if hasattr(entry, "keys_range"):
            keys = await entry.keys_range(offset=0, limit=None)
        else:
            keys = entry.keys()
        body = json.dumps([url + "/" + key for key in keys])

        return Response(body, status_code=200, media_type="application/json")

    elif entry.structure_family == StructureFamily.table:
        # List the columns of the table -- they will be accessed separately as arrays
        body = json.dumps([url + "/" + key for key in entry.structure().columns])

        return Response(body, status_code=200, media_type="application/json")
    
    elif entry.structure_family == StructureFamily.array and isinstance(entry.structure().data_type, StructDtype):
        # List the column names of the structured array -- they will be accessed separately
        body = json.dumps([url + "/" + f.name for f in entry.structure().data_type.fields])

        return Response(body, status_code=200, media_type="application/json")

    elif entry.structure_family in {StructureFamily.array, StructureFamily.sparse}:
        # Return the actual array values for a single block of zarr array
        if block is not None:
            import numpy as np
            from sparse import SparseArray

            zarr_block_indx = [int(i) for i in block.split(",")]
            zarr_block_spec = convert_chunks_for_zarr(entry.structure().chunks)
            if (not (zarr_block_spec == [] and zarr_block_indx == [0])) and (
                len(zarr_block_spec) != len(zarr_block_indx)
            ):
                # Not a scalar and shape doesn't match
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail=f"Requested zarr block index {zarr_block_indx} is inconsistent with the shape of array, {entry.structure().shape}.",  # noqa
                )

            # Indices of the array slices in each dimension that correspond to the requested zarr block
            block_slices = tuple(
                [
                    slice(i * c, (i + 1) * c)
                    for i, c in zip(zarr_block_indx, zarr_block_spec)
                ]
            )
            try:
                with record_timing(request.state.metrics, "read"):
                    array = await ensure_awaitable(entry.read, slice=block_slices)
            except IndexError:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail=f"Index of zarr block {zarr_block_indx} is out of range.",
                )

            if isinstance(array, SparseArray):
                array = array.todense()

            # Padd the last slices with zeros if needed to ensure all zarr blocks have same shapes
            padding_size = [
                max(0, sl.stop - sh)
                for sl, sh in zip(block_slices, entry.structure().shape)
            ]
            if sum(padding_size) > 0:
                array = np.pad(array, [(0, p) for p in padding_size], mode="constant")

            # Ensure the array is contiguous and encode it; equivalent to `buf = zarr.array(array).store['0.0']`
            array = array.astype(array.dtype, order=ZARR_BYTE_ORDER, copy=False)
            buf = zarr_codec.encode(array)
            if not isinstance(buf, bytes):
                buf = array.tobytes(order="A")

            return Response(buf, status_code=200)

        else:
            # TODO:
            # Entire array (root uri) is requested -- never happens, but need to decide what to return here
            return Response(json.dumps({}), status_code=200)
