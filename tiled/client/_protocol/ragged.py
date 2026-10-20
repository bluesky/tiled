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
from typing import Any, Iterable, Union

import httpx
import orjson
import ragged

from ...mimetypes import JSON_MIMETYPE, ZIP_MIMETYPE
from ...ndslice import NDSlice
from ...serialization.ragged import from_zipped_buffers, to_zipped_buffers
from ...structures.ragged import (
    CanonicalRaggedArray,
    RaggedCompatibleType,
    RaggedStructure,
    make_ragged_array,
)
from ...utils import Conflicts
from ..utils import params_from_slice
from .array import parse_patch_response, patch_conflict_message
from .request import RequestDraft

__all__ = [
    "PATCH_CONFLICT_STATUS_CODES",
    "RaggedReadRecipe",
    "draft_patch_request",
    "draft_write_block_request",
    "draft_write_request",
    "parse_patch_response",
    "patch_conflict_exception",
]

# Status codes with which the server reports that patch parameters conflict
# with the existing array (e.g. would overwrite without `extend=True`).
PATCH_CONFLICT_STATUS_CODES = (httpx.codes.BAD_REQUEST, httpx.codes.CONFLICT)


@dataclass(frozen=True)
class RaggedReadRecipe:
    """A recipe for one read of ragged data: the request plus its decoding.

    The server content-negotiates ragged reads: a scalar result -- a 0-dimensional
    array, or a slice selecting a single element -- is served as JSON, anything
    else as zipped awkward buffers. The `Accept` header in the draft and the decoding
    in `parse` reflect one decision, made when the recipe is built.

    Attributes
    ----------
    draft : RequestDraft
        The request fetching the data.
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
        "Recipe reading the whole array or a slice of it, via `/ragged/full`."
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

    def parse(self, content: bytes) -> ragged.array:
        "Decode the response matching this recipe's content negotiation."
        if self.scalar:
            return ragged.array(orjson.loads(content))
        return from_zipped_buffers(buffer=content, structure=self.structure)


def _zipped_buffers_body(array: CanonicalRaggedArray) -> bytes:
    "Encode a ragged array as the zipped-buffers bytes of a request body."
    return to_zipped_buffers(mimetype=ZIP_MIMETYPE, array=array, metadata={})


def draft_write_request(
    link: str, array: RaggedCompatibleType, persist: bool = True
) -> RequestDraft:
    "Draft a request overwriting the array in full, via `PUT /ragged/full`."
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


def draft_write_block_request(
    link: str,
    block: Union[int, Iterable[int]],
    array: CanonicalRaggedArray,
    persist: bool = True,
) -> RequestDraft:
    """Draft a request overwriting one block of the array, via `PUT /ragged/block`.

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


def draft_patch_request(
    link: str,
    array: CanonicalRaggedArray,
    offset: Union[int, tuple[int, ...]],
    extend: bool = False,
    persist: bool = True,
) -> RequestDraft:
    """Draft a request writing into a slice of the array, via `PATCH /ragged/full`.

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


def patch_conflict_exception(status_code: int, body: dict[str, Any]) -> Exception:
    """Translate a patch-conflict response into the exception to raise.

    A 409 (Conflict) maps to `Conflicts`; a 400 (Bad Request) maps to
    `ValueError`. The message rewrite is shared with the array protocol:
    the server phrases patch conflicts identically for both families.
    """
    exception_type = Conflicts if status_code == httpx.codes.CONFLICT else ValueError
    return exception_type(patch_conflict_message(body))
