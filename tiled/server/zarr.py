import dataclasses
import inspect
import os
import re
import warnings
from datetime import datetime, timedelta
from functools import partial, wraps
from pathlib import Path
from typing import Any, List, Optional, Tuple
import json

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
from ..utils import ensure_awaitable, patch_mimetypes, path_from_uri
from ..validation_registration import ValidationError
from . import schemas
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

ZARR_BLOCK_SIZE = 10

router = APIRouter()

def convert_chunks_for_zarr(chunks: Tuple[Tuple[int]]):
    """Convert full chunk specification into zarr format
    
    Zarr only accepts chunks of constant size; this function finds a unique representation of (possibly variable-
    sized chunks) internal to Tiled ArrayAdapter in terms of zarr blocks.
    """
    # return [min(ZARR_BLOCK_SIZE, i[0]) for i in chunks]
    return [ZARR_BLOCK_SIZE for _ in chunks]

def slice_for_zarr_block(chunks: Tuple[Tuple[int]], zblock: Tuple[int]):
    ...

# @router.get("/.zgroup", name="Root .zgroup metadata")
@router.get("/{path:path}/.zgroup", name="Zarr .zgroup metadata")
async def get_zarr_group_metadata(
    request: Request,
    entry=SecureEntry(
        scopes=["read:data", "read:metadata"],
        structure_families={StructureFamily.table, StructureFamily.container},
    ),
):

    return Response(json.dumps({"zarr_format": 2}), status_code=200)

@router.get("/{path:path}/.zarray", name="Zarr .zarray metadata")
async def get_zarr_array_metadata(
    request: Request,
    path: str,
    entry=SecureEntry(scopes=["read:data", "read:metadata"]),
):
    if entry.structure_family not in {StructureFamily.array, StructureFamily.sparse}:
        # This is normal behaviour; zarr will try to open .zarray and, if 404 is received, it will move on assuming
        # that the requested resource is a group (`.../path/.zgroup` would be requested next).
        # TODO: Perhaps, checking this within SecureEntry is sufficient? What happens to tables?
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Requested resource does not have .zarray")

    try:
        zarray_spec = {}
        metadata = entry.metadata()
        structure = entry.structure()
        zarray_spec = {'chunks': convert_chunks_for_zarr(structure.chunks),
            'compressor': {'blocksize': 0,
                'clevel': 5,
                'cname': 'lz4',
                'id': 'blosc',
                'shuffle': 1},
            'dtype': structure.data_type.to_numpy_str(),
            'fill_value': 0,
            'filters': None,
            'order': 'C',
            'shape': list(structure.shape),
            'zarr_format': 2}
    except Exception as err:
        print(f"Can not create .zarray metadata, {err}")
        raise HTTPException(status_code=HTTP_500_INTERNAL_SERVER_ERROR, detail=err.args[0])

    return Response(json.dumps(zarray_spec), status_code=200)


@router.get("/{path:path}", name="Zarr .zgroup directory structure or a chunk of a zarr array")
async def get_zarr_array(
    request: Request,
    block: str | None = None,
    entry=SecureEntry(scopes=["read:data"],
        # structure_families={StructureFamily.array, StructureFamily.sparse},
        # structure_families={StructureFamily.table, StructureFamily.container},
    ),
):
    if entry.structure_family in {StructureFamily.table, StructureFamily.container}:
        # List the contents of a simulated zarr directory (excluding .zarray and .zgroup files)
        url = str(request.url).split('?')[0].rstrip('/')    # Remove query params and trailing slash
        body = json.dumps([url + '/' + key for key in entry.keys()])

        return Response(body, status_code=200, media_type='application/json')

    elif entry.structure_family in {StructureFamily.array, StructureFamily.sparse}:
        if block is not None:
            import zarr
            print(f"Here, {block=}")

            block = [int(i) for i in block.split(',')]
            chunks = entry.structure().chunks

            if block == ():
                # Handle special case of numpy scalar
                with record_timing(request.state.metrics, "read"):
                    array = await ensure_awaitable(entry.read)
            else:
                try:
                    with record_timing(request.state.metrics, "read"):
                        # array = await ensure_awaitable(entry.read_block, block)
                        array = await ensure_awaitable(entry.read)
                        x, y = block
                        array = array[x*ZARR_BLOCK_SIZE:(x+1)*ZARR_BLOCK_SIZE, y*ZARR_BLOCK_SIZE:(y+1)*ZARR_BLOCK_SIZE]
                except IndexError:
                    raise HTTPException(
                        status_code=HTTP_400_BAD_REQUEST, detail="Block index out of range"
                    )
            
            # TODO: This must be cached!
            zarray = zarr.array(array)

            return Response(zarray.store['0.0'], status_code=200)

        else:
            # TODO:
            # Entire array (root uri) is requested -- never happens, but need to decide what to return here
            return Response(json.dumps({}), status_code=200)

