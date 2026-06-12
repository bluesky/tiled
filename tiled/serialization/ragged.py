from __future__ import annotations

import io
import zipfile

import awkward
import numpy
import orjson
import ragged

from ..media_type_registration import (
    default_deserialization_registry,
    default_serialization_registry,
)
from ..mimetypes import APACHE_ARROW_FILE_MIME_TYPE, PARQUET_MIMETYPE
from ..serialization import awkward as awkward_serialization
from ..structures.core import StructureFamily
from ..structures.ragged import CanonicalRaggedArray, RaggedStructure, make_ragged_array
from ..utils import SerializationError, modules_available, safe_json_dump


def _buffers_from_data(form: awkward.forms.Form, data):
    """Construct Awkward buffers from nested data matching the supplied Canonical Ragged form

    Returns
    -------
    length : int
        Top-level array length.
    buffers : dict[str, numpy.ndarray]
        Buffers suitable for awkward.from_buffers(form, length, buffers).
    """

    buffers = {}

    def recurse(form, data):
        if isinstance(form, awkward.forms.NumpyForm):
            arr = numpy.asarray(data, dtype=numpy.dtype(form.primitive))
            buffers[f"{form.form_key}-data"] = arr.ravel()
            return arr.shape[0] if getattr(form, "inner_shape", ()) else len(arr)

        if isinstance(form, awkward.forms.RegularForm):
            flattened = []
            for row in data:
                if len(row) != form.size:
                    raise ValueError(
                        f"Row width mismatch for fixed-size dimension "
                        f"(form key {form.form_key!r}): expected {form.size}, got {len(row)}"
                    )
                flattened.extend(row)
            recurse(form.content, flattened)
            return len(data)

        if isinstance(form, awkward.forms.ListOffsetForm):
            offsets, flattened = [0], []
            for row in data:
                offsets.append(offsets[-1] + len(row))
                flattened.extend(row)

            offsets = numpy.asarray(offsets, dtype=numpy.int64)
            buffers[f"{form.form_key}-offsets"] = offsets
            recurse(form.content, flattened)

            return len(data)

        raise TypeError(f"Unsupported form type: {type(form).__name__}")

    return recurse(form, data), buffers


@default_serialization_registry.register(StructureFamily.ragged, "application/json")
def to_json(
    mimetype: str,
    array: CanonicalRaggedArray,
    metadata: dict,  # noqa: ARG001
) -> bytes:
    return safe_json_dump(array.tolist())


@default_deserialization_registry.register(StructureFamily.ragged, "application/json")
def from_json(
    contents: str | bytes, structure: RaggedStructure
) -> CanonicalRaggedArray:
    lists_of_lists = orjson.loads(contents)
    form = structure.awkward_form
    length, buffers = _buffers_from_data(form, lists_of_lists)

    return make_ragged_array(awkward.from_buffers(form, length, buffers))


@default_serialization_registry.register(StructureFamily.ragged, "application/zip")
def to_zipped_buffers(
    mimetype: str,
    array: CanonicalRaggedArray,
    metadata: dict,  # noqa: ARG001
) -> bytes:
    if array.ndim == 0:
        raise SerializationError("Cannot serialize scalar values to zipped buffers")
    (form, length, buffers) = awkward.to_buffers(array._impl)
    file = io.BytesIO()
    with zipfile.ZipFile(file, "w", compresslevel=zipfile.ZIP_STORED) as fzip:
        for key, buf in buffers.items():
            fzip.writestr(key, buf.tobytes())
        # Encode as fixed 8-byte unsigned big-endian. We pin the byte length
        # explicitly because Python 3.10's ``int.to_bytes`` requires it (the
        # parameter only became optional in 3.11). 8 bytes is symmetric with
        # ``int.from_bytes`` on the read side and accommodates any plausible
        # awkward array length.
        fzip.writestr("length", length.to_bytes(8, byteorder="big"))
        fzip.writestr("form", safe_json_dump(form.to_json()))

    return file.getvalue()


@default_deserialization_registry.register(StructureFamily.ragged, "application/zip")
def from_zipped_buffers(
    buffer: bytes, structure: RaggedStructure
) -> CanonicalRaggedArray:  # noqa: ARG001
    file = io.BytesIO(buffer)
    with zipfile.ZipFile(file, "r") as fzip:
        form = awkward.forms.from_json(orjson.loads(fzip.read("form")))
        form_keys = form.expected_from_buffers()
        buffers = {
            key: numpy.frombuffer(fzip.read(key), dtype=typ)
            for key, typ in form_keys.items()
        }
        length = int.from_bytes(fzip.read("length"), byteorder="big")

    return make_ragged_array(awkward.from_buffers(form, length, buffers))


if modules_available("pyarrow"):

    @default_serialization_registry.register(
        StructureFamily.ragged, APACHE_ARROW_FILE_MIME_TYPE
    )
    def to_arrow(mimetype: str, array: ragged.array, metadata: dict):
        if array.ndim == 0:
            raise SerializationError("Cannot serialize scalar value to Arrow.")
        components = awkward.to_buffers(array._impl)
        return awkward_serialization.to_arrow(mimetype, components, metadata)

    @default_serialization_registry.register(StructureFamily.ragged, PARQUET_MIMETYPE)
    def to_parquet(mimetype: str, array: ragged.array, metadata: dict):
        if array.ndim == 0:
            raise SerializationError("Cannot serialize scalar value to Parquet.")
        components = awkward.to_buffers(array._impl)
        return awkward_serialization.to_parquet(mimetype, components, metadata)
