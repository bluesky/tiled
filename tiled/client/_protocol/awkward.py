"""Pure request builders and response parsers for the awkward client.

Reads are described by `AwkwardReadRecipe`: a single POST to the buffers
endpoint requesting, by form key, only the buffers the requested slice
touches, plus the projected form and length needed to decode the zipped
buffers in the response. Writes have nothing to decode, so they are
described by plain `RequestDraft`s (see the package docstring for the
convention).

No I/O happens in this module; the client class sends the drafts and feeds
response content back through the parser function.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import awkward
import orjson

from ...mimetypes import JSON_MIMETYPE, ZIP_MIMETYPE
from ...serialization.awkward import from_zipped_buffers, to_zipped_buffers
from ...structures.awkward import AwkwardStructure, project_form
from .request import RequestDraft


@dataclass(frozen=True)
class AwkwardReadRecipe:
    """A recipe for one read of awkward data: the request plus decoding context.

    Attributes
    ----------
    draft : RequestDraft
        POST to the buffers endpoint, with the form keys to fetch as a JSON
        list in the body.
    form : awkward.forms.Form
        The structure's form projected onto the touched form keys. The
        response carries only raw buffers, so the form must travel with the
        draft to interpret them.
    length : int
        Top-level length of the array.
    """

    draft: RequestDraft
    form: awkward.forms.Form
    length: int

    def parse(self, content: bytes) -> awkward.Array:
        "Decode the response for this read."
        return parse_buffers_response(content, self.form, self.length)

    @classmethod
    def for_buffers(
        cls, link: str, structure: AwkwardStructure, slice: Any = ...
    ) -> "AwkwardReadRecipe":
        """Recipe reading the buffers one slice touches, via `POST /awkward/buffers`.

        A typetracer pass over the slice determines which form keys (buffers)
        it touches. Only those buffers are requested, and the form is
        projected onto them for decoding the response.
        """
        form = awkward.forms.from_dict(structure.form)
        typetracer, report = awkward.typetracer.typetracer_with_report(form)
        proxy_array = awkward.Array(typetracer)
        awkward.typetracer.touch_data(proxy_array[slice])
        form_keys_touched = set(report.data_touched)
        projected_form = project_form(form, form_keys_touched)
        # The order is not important, but sort so that the request is deterministic.
        form_keys = sorted(form_keys_touched)
        draft = RequestDraft(
            "POST",
            link,
            headers={"Accept": ZIP_MIMETYPE, "Content-Type": JSON_MIMETYPE},
            content=orjson.dumps(form_keys),
        )
        return cls(draft, projected_form, structure.length)


def parse_buffers_response(
    content: bytes, form: awkward.forms.Form, length: int
) -> awkward.Array:
    "Decode a zipped-buffers response into an awkward array with the given form."
    container = from_zipped_buffers(content, form, length)
    return awkward.from_buffers(form, length, container, allow_noncanonical_form=True)


def draft_write_request(
    link: str, structure: AwkwardStructure, container: Any
) -> RequestDraft:
    "Draft a request overwriting the array in full, via `PUT /awkward/full`."
    components = (structure.form, structure.length, container)
    return RequestDraft(
        "PUT",
        link,
        headers={"Content-Type": ZIP_MIMETYPE},
        content=bytes(to_zipped_buffers(ZIP_MIMETYPE, components, {})),
    )
