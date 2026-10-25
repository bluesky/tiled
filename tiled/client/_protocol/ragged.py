"""Pure request builders and response parsers for the ragged client.

Reads are described by `RaggedReadRecipe`: the request draft to send plus the
content-negotiation decision that selects both the `Accept` header in the draft
and the decoding of the response. Writes have nothing to decode, so they are
described by plain `RequestDraft`s (see the package docstring for the convention).

No I/O happens in this module; the client class sends the request drafts and feeds
response content back through the parsers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence, Union

import orjson
import ragged

from ...mimetypes import JSON_MIMETYPE, ZIP_MIMETYPE
from ...ndslice import NDSlice
from ...serialization.ragged import from_zipped_buffers, to_zipped_buffers
from ...structures.core import StructureFamily
from ...structures.data_source import DataSource
from ...structures.ragged import (
    CanonicalRaggedArray,
    RaggedCompatibleType,
    RaggedStructure,
    make_ragged_array,
)
from ..utils import params_from_slice
from .request import RequestDraft


@dataclass(frozen=True)
class RaggedReadRecipe:
    """A recipe for one read of ragged data: the request plus its decoding.

    The server content-negotiates ragged reads: a scalar result -- a 0-dimensional
    array, or a slice selecting a single element -- is served as JSON, anything
    else as zipped awkward buffers. The `Accept` header in the draft and the decoding
    in `parse_response` reflect one decision, made when the recipe is built.

    Attributes
    ----------
    draft : RequestDraft
        The request that fetches the data.
    scalar : bool
        Whether the result is a scalar, served as JSON.
    structure : RaggedStructure
        Describes the buffers of a non-scalar response.
    """

    draft: RequestDraft
    scalar: bool
    structure: RaggedStructure

    @classmethod
    def for_full(
        cls, link: str, structure: RaggedStructure, slice: Any = None
    ) -> "RaggedReadRecipe":
        "A recipe that reads the whole array or a slice of it, via `/ragged/full`."
        scalar = (len(structure.shape) == 0) or NDSlice(slice).is_scalar(
            structure.shape
        )
        draft = RequestDraft(
            "GET",
            link,
            params=params_from_slice(slice),
            headers={"Accept": JSON_MIMETYPE if scalar else ZIP_MIMETYPE},
        )
        return cls(draft, scalar, structure)

    def parse_response(self, content: bytes) -> ragged.array:
        "Decode the response according to this recipe's content negotiation."
        if self.scalar:
            return ragged.array(orjson.loads(content))
        return from_zipped_buffers(buffer=content, structure=self.structure)


def _zipped_buffers_body(array: CanonicalRaggedArray) -> bytes:
    "Encode a ragged array as the zipped-buffers bytes of a request body."
    return to_zipped_buffers(mimetype=ZIP_MIMETYPE, array=array, metadata={})


def draft_request_write_ragged_full(
    link: str, array: RaggedCompatibleType, persist: bool = True
) -> RequestDraft:
    "Draft the request that overwrites the array in full, via `PUT /ragged/full`."
    array = make_ragged_array(array)
    params: dict[str, Any] = {}
    if persist is False:
        # Extend the query only for non-default behavior.
        params["persist"] = persist
    return RequestDraft(
        "PUT",
        link,
        params=params,
        headers={"Content-Type": ZIP_MIMETYPE},
        content=_zipped_buffers_body(array),
    )


def draft_request_write_ragged_block(
    link: str,
    block: Union[int, Iterable[int]],
    array: CanonicalRaggedArray,
    persist: bool = True,
) -> RequestDraft:
    """Draft the request that overwrites one block of the array, via `PUT /ragged/block`.

    The block index is sent as a comma-joined `block` query parameter,
    overriding the placeholder template baked into the server-provided link.
    """
    block_tuple = (block,) if isinstance(block, int) else tuple(block)
    params: dict[str, Any] = {"block": ",".join(map(str, block_tuple))}
    if persist is False:
        params["persist"] = persist
    return RequestDraft(
        "PUT",
        link,
        params=params,
        headers={"Content-Type": ZIP_MIMETYPE},
        content=_zipped_buffers_body(array),
    )


def draft_request_patch_ragged_full(
    link: str,
    array: CanonicalRaggedArray,
    offset: Union[int, tuple[int, ...]],
    extend: bool = False,
    persist: bool = True,
) -> RequestDraft:
    """Draft the request that writes into a slice of the array, via `PATCH /ragged/full`.

    `shape` carries only the leading dimension: a ragged array grows along
    dimension 0 and its inner dimensions are variable.
    """
    if isinstance(offset, int):
        offset = (offset,)
    params: dict[str, Any] = {
        "shape": str(array.shape[0]),
        "offset": ",".join(map(str, offset)),
        "extend": bool(extend),
    }
    if persist is False:
        # Extend the query only for non-default behavior.
        params["persist"] = persist
    return RequestDraft(
        "PATCH",
        link,
        params=params,
        headers={"Content-Type": ZIP_MIMETYPE},
        content=_zipped_buffers_body(array),
    )


def data_source_from_ragged(
    array: Any, dims: Optional[Sequence[str]], max_partition_bytes: int
) -> DataSource:
    """The data source that describes a ragged array upload.

    `array` must already be in canonical ragged form (`make_ragged_array`).
    Chunks are chosen such that each partition is not too large to upload
    (`max_partition_bytes`).
    """
    from ...structures.ragged import make_ragged_chunks

    chunks = make_ragged_chunks(array, max_partition_bytes)
    return DataSource(
        structure=RaggedStructure.from_array(array, chunks=chunks, dims=dims),
        structure_family=StructureFamily.ragged,
        mimetype="application/x-ragged+sql",
    )
