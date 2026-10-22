"""Pure request builders and response parsers for the array client.

Reads of array data -- whole, sliced, or single-block -- are described by
`ArrayReadRecipe`: the request draft(s) to send plus the `dtype` and the
tiling needed to decode and assemble the responses. Writes have nothing to
decode and no short-circuit, so they are described by plain `RequestDraft`s
(see the package docstring for the convention).

No I/O happens in this module; the client classes send the drafts and feed
response content back through the parser functions.
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass
from typing import Any, Optional, Sequence, Union

import numpy
from numpy.typing import DTypeLike, NDArray

from ...mimetypes import OCTET_STREAM_MIMETYPE
from ...ndslice import NDBlock, NDSlice, split_slice
from ...structures.array import ArrayStructure
from ...structures.core import StructureFamily
from ...structures.data_source import DataSource
from ...type_aliases import Chunks
from ..utils import params_from_slice, slices_to_dask_chunks
from .request import RequestDraft, fill_link_template


@dataclass(frozen=True)
class ArrayReadRecipe:
    """A recipe for one logical read of array data: requests plus assembly.

    Covers whole-array, sliced, and single-block reads uniformly. `tiling`
    is the single source of truth for shapes: the shape of each draft's
    sub-result and of the assembled result are both derived from it.

    Attributes
    ----------
    drafts : dict
        Maps a tile index to the request fetching the sub-result for that
        tile. A read small enough for a single request has one entry; a read
        whose expected size exceeds the byte-size limit is split into several;
        a read whose shape contains 0 has none (an array with a 0-length
        dimension never contains data) and assembles to an empty array locally.
    dtype : numpy.dtype
        How to interpret the raw response bytes.
    tiling : Chunks
        How the drafts' sub-results tile the assembled result, one tuple
        of sizes per dimension (the convention dask calls `chunks`). This
        describes the *read*, not the stored array: it is generally NOT
        `ArrayStructure.chunks` (the array's storage chunking), although
        a split read prefers storage-chunk boundaries as split points.
    """

    drafts: dict[tuple[int, ...], RequestDraft]
    dtype: numpy.dtype
    tiling: Chunks

    @property
    def shape(self) -> tuple[int, ...]:
        "Shape of the assembled result."
        return tuple(sum(axis_sizes) for axis_sizes in self.tiling)

    def shape_of_draft(self, index: tuple[int, ...]) -> tuple[int, ...]:
        "Shape of the sub-result fetched by the draft at one tile index."
        return tuple(self.tiling[dim][i] for dim, i in enumerate(index))

    def assemble(self, results: dict[tuple[int, ...], NDArray[Any]]) -> NDArray[Any]:
        """Place the fetched sub-results, keyed by tile index, into one array.

        `results` must hold one sub-result per draft. The degenerate tilings
        require no stitching: with no tiles (a zero-size read) the empty
        result is constructed locally, and with a single tile the one
        sub-result is the result (returned as-is, without a copy).
        """
        if not results:
            return self.empty()
        if len(results) == 1:
            (result,) = results.values()
            return result
        out = numpy.empty(self.shape, dtype=self.dtype)
        starts = tuple(
            tuple(itertools.accumulate(axis_sizes, initial=0))
            for axis_sizes in self.tiling
        )
        for index, result in results.items():
            region = tuple(
                slice(starts[dim][i], starts[dim][i] + self.tiling[dim][i])
                for dim, i in enumerate(index)
            )
            out[region] = result
        return out

    @classmethod
    def for_block(
        cls,
        link: str,
        structure: ArrayStructure,
        block: NDBlock,
        block_slice: Optional[NDSlice] = None,
    ) -> "ArrayReadRecipe":
        """Recipe reading one block (chunk) of an array, via `/array/block`.

        The block boundaries are determined by `structure.chunks`, and usually
        correspond to the chunking of the array on the server side. Optionally,
        `block_slice` selects a slice within the block. The recipe always has a
        single tile (i.e. one request draft, or none for a 0-size block).
        """
        dtype = structure.data_type.to_numpy_dtype()
        storage_chunks = structure.chunks
        # Determine the expected shape of the resulting array after slicing
        expected_shape: tuple[int, ...] = ()
        # Expand the block to convert for URL
        block = block.expand_for_shape(tuple(len(dim) for dim in storage_chunks))
        if shape := block.shape_from_chunks(storage_chunks):
            expected_shape = (
                block_slice.shape_after_slice(shape) if block_slice else shape
            )
        single_tile = tuple((size,) for size in expected_shape)
        if 0 in expected_shape:
            return cls({}, dtype, single_tile)
        params = {
            "block": block.to_numpy_str(),
            "expected_shape": ",".join(map(str, expected_shape)) or "scalar",
        }
        if block_slice:
            params["slice"] = block_slice.to_numpy_str()
        draft = RequestDraft(
            "GET",
            link,
            params=params,
            headers={"Accept": OCTET_STREAM_MIMETYPE},
        )
        return cls({(0,) * len(expected_shape): draft}, dtype, single_tile)

    @classmethod
    def for_full(
        cls,
        link: str,
        structure: ArrayStructure,
        array_slice: NDSlice,
        bytesize_limit: int,
    ) -> "ArrayReadRecipe":
        """Recipe reading the whole array or a slice of it, via `/array/full`.

        If a read is expected to exceed `bytesize_limit`, it is split into multiple fetches,
        using chunk boundaries along each axis as the best candidate split points.
        """
        dtype = structure.data_type.to_numpy_dtype()
        if array_slice:
            array_slice = array_slice.expand_for_shape(structure.shape)  # Remove "..."
        expected_shape = array_slice.shape_after_slice(structure.shape)
        single_tile = tuple((size,) for size in expected_shape)
        if 0 in expected_shape:
            return cls({}, dtype, single_tile)
        total_bytes = math.prod(expected_shape) * dtype.itemsize
        if total_bytes < bytesize_limit:
            draft = cls._draft_request_read_full(link, array_slice, expected_shape)
            return cls({(0,) * len(expected_shape): draft}, dtype, single_tile)
        chunk_bounds = tuple(
            tuple(itertools.accumulate(axis_chunks, initial=0))
            for axis_chunks in structure.chunks
        )
        indexed_slices = split_slice(
            # Idempotent for a slice already expanded above; expands the
            # (falsy) whole-array slice, which is sent unexpanded when small.
            array_slice.expand_for_shape(structure.shape),
            max_size=bytesize_limit // dtype.itemsize,
            pref_splits=chunk_bounds,
        )
        drafts = {
            index: cls._draft_request_read_full(
                link, subslice, subslice.shape_after_slice(structure.shape)
            )
            for index, subslice in indexed_slices.items()
        }
        return cls(
            drafts,
            dtype,
            # The sub-slices address the full array, so chunk sizes are computed
            # against the full `structure.shape`.
            slices_to_dask_chunks(indexed_slices, structure.shape),
        )

    @staticmethod
    def _draft_request_read_full(
        link: str,
        array_slice: NDSlice,
        expected_shape: tuple[int, ...],
    ) -> RequestDraft:
        "Draft the request fetching one (sub-)slice of the array, via `/array/full`."
        params: dict[str, Any] = {
            "expected_shape": ",".join(map(str, expected_shape)) or "scalar"
        }
        if array_slice:
            params["slice"] = array_slice.to_numpy_str()
        return RequestDraft(
            "GET",
            link,
            params=params,
            headers={"Accept": OCTET_STREAM_MIMETYPE},
        )

    @staticmethod
    def parse(content: bytes, dtype: DTypeLike, shape: tuple[int, ...]) -> NDArray[Any]:
        """Convert a raw `octet-stream` response body into a numpy array.

        A staticmethod, unlike the sibling recipes' `parse`: the decode
        parameters are passed as arguments so that dask tasks can carry
        primitives and reference this function by qualified name, without
        embedding the recipe.
        """
        return numpy.frombuffer(content, dtype=dtype).reshape(shape)

    def empty(self) -> NDArray[Any]:
        """The (empty) result of a read short-circuited client-side.

        An array with 0 in its shape contains no data, so no request is
        needed (`drafts` is empty).
        """
        return numpy.array([], dtype=self.dtype).reshape(self.shape)


def draft_request_read_referenced_array(uri: str) -> RequestDraft:
    """Draft the request fetching the array that a stream update references.

    Streaming updates may reference array data by URI (`array-ref` messages)
    instead of carrying an inline payload. The URI arrives complete, with any
    selection already baked in by the server, so it is fetched as-is, as raw
    octet-stream bytes (see `ArrayReadRecipe.parse`).
    """
    return RequestDraft("GET", uri, headers={"Accept": OCTET_STREAM_MIMETYPE})


def draft_request_write_array_full(
    link: str, array: Any, persist: bool = True
) -> RequestDraft:
    "Draft a request overwriting the full array, via `PUT /array/full`."
    if not (hasattr(array, "shape") and hasattr(array, "dtype")):
        array = numpy.asarray(array)
    params: dict[str, Any] = {}
    if persist is False:
        # Extend the query only for non-default behavior.
        params["persist"] = persist
    return RequestDraft(
        "PUT",
        link,
        params=params,
        headers={"Content-Type": OCTET_STREAM_MIMETYPE},
        content=array.tobytes(),
    )


def draft_request_write_array_block(
    link_template: str,
    block: tuple[int, ...],
    array: Any,
    slice: Any = ...,
    persist: bool = True,
) -> RequestDraft:
    """Draft a request overwriting one block of the array, via `PUT /array/block`.

    The server provides the block link as a URL template with one positional
    placeholder per dimension, filled in with the block index.
    """
    if not (hasattr(array, "shape") and hasattr(array, "dtype")):
        array = numpy.asarray(array)
    url = fill_link_template(link_template, *block)
    params = params_from_slice(slice)
    if persist is False:
        # Extend the query only for non-default behavior.
        params["persist"] = persist
    return RequestDraft(
        "PUT",
        url,
        params=params,
        headers={"Content-Type": OCTET_STREAM_MIMETYPE},
        content=array.tobytes(),
    )


def draft_request_patch_array_full(
    link: str,
    array: NDArray[Any],
    offset: Union[int, tuple[int, ...]],
    extend: bool = False,
    persist: bool = True,
) -> RequestDraft:
    "Draft a request writing into a slice of the array, via `PATCH /array/full`."
    array_ = numpy.ascontiguousarray(array)
    if isinstance(offset, int):
        offset = (offset,)
    params: dict[str, Any] = {
        "offset": ",".join(map(str, offset)),
        "shape": ",".join(map(str, array_.shape)),
        "extend": bool(extend),
    }
    if persist is False:
        # Extend the query only for non-default behavior.
        params["persist"] = persist
    return RequestDraft(
        "PATCH",
        link,
        params=params,
        headers={"Content-Type": OCTET_STREAM_MIMETYPE},
        content=array_.tobytes(),
    )


def data_source_from_array(
    array: Any, dims: Optional[Sequence[str]], max_chunk_bytes: int
) -> DataSource:
    """The data source describing an array upload.

    Chunks are chosen such that each chunk is not too large to upload
    (`max_chunk_bytes`), taking any existing chunking into account. If the
    array is small, there will be only one chunk. `array` must expose `shape`,
    `dtype`, and optionally `chunks`. The dask import is deferred: it is only
    needed when writing.
    """
    from dask.array.core import normalize_chunks

    # Take any existing chunking into account; otherwise let dask choose.
    requested = (
        array.chunks if hasattr(array, "chunks") else tuple("auto" for _ in array.shape)
    )
    chunks = normalize_chunks(
        requested,
        limit=max_chunk_bytes,
        dtype=array.dtype,
        shape=array.shape,
    )
    return DataSource(
        structure=ArrayStructure.from_array(array, chunks=chunks, dims=dims),
        structure_family=StructureFamily.array,
    )
