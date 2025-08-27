import json
import re
from typing import Optional, Set, Tuple, Union

import numcodecs
import orjson
import pydantic_settings
from fastapi import APIRouter, Depends, HTTPException, Request
from starlette.responses import Response
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_500_INTERNAL_SERVER_ERROR

from ..structures.core import StructureFamily
from ..type_aliases import Scopes
from ..utils import ensure_awaitable
from .authentication import (
    get_current_access_tags,
    get_current_principal,
    get_current_scopes,
    get_session_state,
)
from .dependencies import get_entry, get_root_tree
from .schemas import Principal
from .utils import record_timing

ZARR_BLOCK_SIZE = 10000
ZARR_BYTE_ORDER = "C"
ZARR_CODEC_SPEC = {
    "blocksize": 0,
    "clevel": 5,
    "cname": "lz4",
    "id": "blosc",
    "shuffle": 1,
}

zarr_codec = numcodecs.get_codec(ZARR_CODEC_SPEC)


def convert_chunks_for_zarr(tiled_chunks: Tuple[Tuple[int]]):
    """Convert full tiled/dask chunk specification into zarr format

    Zarr only accepts chunks of constant size along each dimension; this function
    finds a unique representation of (possibly variable-sized chunks) internal to
    Tiled ArrayAdapter in terms of zarr blocks.

    Zarr chunks must be at least of size 1, even for zero-dimensional arrays.
    """
    return [min(ZARR_BLOCK_SIZE, max(*tc, 1)) for tc in tiled_chunks]


def get_zarr_router_v2() -> APIRouter:
    router = APIRouter()

    @router.get("{path:path}.zattrs", name="Zarr .zattrs metadata")
    @router.get("/{path:path}/.zattrs", name="Zarr .zattrs metadata")
    async def get_zarr_attrs(
        request: Request,
        path: str,
        principal: Union[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
    ):
        "Return entry.metadata as Zarr attributes metadata (.zattrs)"
        entry = await get_entry(
            path,
            ["read:data", "read:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            metrics=request.state.metrics,
            structure_families={
                StructureFamily.table,
                StructureFamily.container,
                StructureFamily.array,
                StructureFamily.sparse,
            },
            access_policy=getattr(request.app.state, "access_policy", None),
        )

        return Response(
            json.dumps(entry.metadata().get("attributes", {})),
            status_code=200,
            media_type="application/json",
        )

    @router.get("{path:path}.zgroup", name="Root .zgroup metadata")
    @router.get("/{path:path}/.zgroup", name="Zarr .zgroup metadata")
    async def get_zarr_group_metadata(
        request: Request,
        path: str,
        principal: Union[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
    ):
        await get_entry(
            path,
            ["read:data", "read:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            metrics=request.state.metrics,
            structure_families={
                StructureFamily.table,
                StructureFamily.container,
            },
            access_policy=getattr(request.app.state, "access_policy", None),
        )

        return Response(json.dumps({"zarr_format": 2}), status_code=200)

    @router.get("/{path:path}/.zarray", name="Zarr .zarray metadata")
    async def get_zarr_array_metadata(
        request: Request,
        path: str,
        principal: Union[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
    ):
        entry = await get_entry(
            path,
            ["read:data", "read:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            metrics=request.state.metrics,
            structure_families={StructureFamily.array, StructureFamily.sparse},
            access_policy=getattr(request.app.state, "access_policy", None),
        )
        structure = entry.structure()
        try:
            zarray_spec = {
                "chunks": convert_chunks_for_zarr(structure.chunks),
                "compressor": zarr_codec.get_config(),
                "dtype": structure.data_type.to_numpy_descr(),
                "fill_value": None,
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
        "/{path:path}",
        name="Zarr group (directory) structure or a chunk of a zarr array",
    )
    async def get_zarr_array(
        request: Request,
        path: str,
        principal: Union[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
    ):
        # If a zarr block is requested, e.g. http://localhost:8000/zarr/v2/array/0.1.2.3,
        # extract it from last part of the path; use the remaining path to get the entry.
        zarr_block_indx = None
        if block := path.strip("/").split("/")[-1]:
            if re.fullmatch(r"^(?:\d+\.)*\d+$", block):
                zarr_block_indx = [int(i) for i in block.split(".")]
                path = path[: -len(block)].rstrip("/")

        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            metrics=request.state.metrics,
            structure_families={
                StructureFamily.array,
                StructureFamily.sparse,
                StructureFamily.table,
                StructureFamily.container,
            },
            access_policy=getattr(request.app.state, "access_policy", None),
        )

        if entry.structure_family == StructureFamily.container:
            # List the contents of a "simulated" zarr directory (excluding .zarray and .zgroup files)
            if hasattr(entry, "keys_range"):
                keys = await entry.keys_range(offset=0, limit=None)
            else:
                keys = entry.keys()
            url = str(request.url).rstrip("/")
            body = json.dumps([url + "/" + key for key in keys])

            return Response(body, status_code=200)

        elif entry.structure_family == StructureFamily.table:
            # List the columns of the table -- they will be accessed separately as arrays
            url = str(request.url).rstrip("/")
            body = json.dumps([url + "/" + key for key in entry.structure().columns])

            return Response(body, status_code=200)

        elif entry.structure_family in {StructureFamily.array, StructureFamily.sparse}:
            # Return the actual array values for a single block of zarr array
            if zarr_block_indx is not None:
                import numpy as np
                from sparse import SparseArray

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

                # Pad the last slices with zeros if needed to ensure all zarr blocks have same shapes
                padding_size = [
                    max(0, sl.stop - sh)
                    for sl, sh in zip(block_slices, entry.structure().shape)
                ]
                if sum(padding_size) > 0:
                    array = np.pad(
                        array, [(0, p) for p in padding_size], mode="constant"
                    )

                # Ensure the array is contiguous and encode it; equivalent to `buf=zarr.array(array).store['0.0']`
                array = array.astype(array.dtype, order=ZARR_BYTE_ORDER, copy=False)
                buf = zarr_codec.encode(array)
                if not isinstance(buf, bytes):
                    buf = array.tobytes(order="A")

                return Response(buf, status_code=200)

            else:
                # TODO:
                # Entire array (root uri) is requested -- never happens, but need to decide what to return here
                return Response(json.dumps({}), status_code=200)

    return router


def get_zarr_router_v3() -> APIRouter:
    router = APIRouter()

    @router.get("/{path:path}/zarr.json", name="Zarr v3 group or array metadata")
    @router.get("/{path:path}zarr.json", name="Zarr v3 group or array metadata")
    async def get_zarr_metadata(
        request: Request,
        path: str,
        principal: Union[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
    ):
        from zarr.dtype import parse_data_type

        entry = await get_entry(
            path,
            ["read:data", "read:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            metrics=request.state.metrics,
            structure_families={
                StructureFamily.array,
                StructureFamily.table,
                StructureFamily.sparse,
                StructureFamily.container,
            },
            access_policy=getattr(request.app.state, "access_policy", None),
        )

        # Array or sparse array
        if entry.structure_family in {StructureFamily.array, StructureFamily.sparse}:
            structure = entry.structure()
            zarr_dtype = parse_data_type(
                structure.data_type.to_numpy_descr(), zarr_format=3
            )
            fill_value = (
                zarr_dtype.default_scalar() if structure.shape else entry.read()[()]
            )
            result = {
                "zarr_format": 3,
                "node_type": "array",
                "shape": list(structure.shape),
                "data_type": zarr_dtype.to_json(zarr_format=3),
                "chunk_grid": {
                    "name": "regular",
                    "configuration": {
                        "chunk_shape": convert_chunks_for_zarr(structure.chunks)
                    },
                },
                "chunk_key_encoding": {
                    "name": "default",
                    "configuration": {"separator": "/"},
                },
                "fill_value": zarr_dtype.to_json_scalar(fill_value, zarr_format=3),
                "codecs": [
                    {"name": "bytes", "configuration": {"endian": "little"}},
                    {
                        "name": zarr_codec.codec_id,
                        "configuration": {
                            "typesize": zarr_dtype.item_size,
                            **{
                                k: v
                                if k != "shuffle"
                                else {1: "shuffle", 0: "no_shuffle", 2: "bitshuffle"}[v]
                                for k, v in zarr_codec.get_config().items()
                                if k != "id"
                            },
                        },
                    },
                ],
                "dimension_names": list(structure.dims) if structure.dims else None,
                "attributes": entry.metadata(),
            }

        elif entry.structure_family in {
            StructureFamily.container,
            StructureFamily.table,
        }:
            # Structured numpy array, Container, or Table
            result = {
                "zarr_format": 3,
                "node_type": "group",
                "attributes": entry.metadata(),
            }

        return Response(
            orjson.dumps(result, option=orjson.OPT_SERIALIZE_NUMPY),
            status_code=200,
        )

    @router.get("/{path:path}/c/{block:path}", name="A chunk of a zarr array")
    async def get_zarr_array(
        request: Request,
        path: str,
        block: str,
        principal: Union[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
    ):
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            metrics=request.state.metrics,
            structure_families={
                StructureFamily.array,
                StructureFamily.sparse,
            },
            access_policy=getattr(request.app.state, "access_policy", None),
        )

        if block is not None:
            import numpy as np
            from sparse import SparseArray

            zarr_block_indx = [int(i) for i in block.split("/")]
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

            # Pad the last slices with zeros if needed to ensure all zarr blocks have same shapes
            padding_size = [
                max(0, sl.stop - sh)
                for sl, sh in zip(block_slices, entry.structure().shape)
            ]
            if sum(padding_size) > 0:
                array = np.pad(array, [(0, p) for p in padding_size], mode="constant")

            # Ensure the array is contiguous and encode it; equivalent to `buf=zarr.array(array).store['0.0']`
            array = array.astype(array.dtype, order=ZARR_BYTE_ORDER, copy=False)
            buf = zarr_codec.encode(array)
            if not isinstance(buf, bytes):
                buf = array.tobytes(order="A")

            return Response(buf, status_code=200)

        else:
            # TODO:
            # Entire array (root uri) is requested -- never happens, but need to decide what to return here
            return Response(json.dumps({}), status_code=200)

    @router.get("/{path:path}", name="Contents of a zarr group")
    async def get_zarr_group(
        request: Request,
        path: str,
        principal: Union[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
    ):
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            metrics=request.state.metrics,
            structure_families={
                StructureFamily.array,
                StructureFamily.container,
                StructureFamily.sparse,
                StructureFamily.table,
            },
            access_policy=getattr(request.app.state, "access_policy", None),
        )
        # Remove query params and the trailing slash from the url
        url = str(request.url).split("?")[0].rstrip("/")

        if entry.structure_family == StructureFamily.container:
            # List the contents of a "simulated" zarr directory (excluding .zarray and .zgroup files)
            if hasattr(entry, "keys_range"):
                keys = await entry.keys_range(offset=0, limit=None)
            else:
                keys = entry.keys()
            body = json.dumps([url + "/" + key for key in keys])

            return Response(body, status_code=200)

        elif entry.structure_family == StructureFamily.table:
            # List the columns of the table -- they will be accessed separately as arrays
            body = json.dumps([url + "/" + key for key in entry.structure().columns])

            return Response(body, status_code=200)

        else:
            return await get_zarr_metadata(
                request,
                path,
                principal=principal,
                authn_access_tags=authn_access_tags,
                authn_scopes=authn_scopes,
                root_tree=root_tree,
                session_state=session_state,
            )

    return router
