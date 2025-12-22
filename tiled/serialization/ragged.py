from __future__ import annotations

import awkward
import numpy as np
import orjson
import ragged

from tiled.media_type_registration import (
    default_deserialization_registry,
    default_serialization_registry,
)
from tiled.mimetypes import APACHE_ARROW_FILE_MIME_TYPE, PARQUET_MIMETYPE
from tiled.serialization import awkward as awkward_serialization
from tiled.structures.core import StructureFamily
from tiled.utils import modules_available, safe_json_dump


@default_serialization_registry.register(StructureFamily.ragged, "application/json")
def to_json(
    mimetype: str, array: ragged.array, metadata: dict  # noqa: ARG001
) -> bytes:
    return safe_json_dump(array.tolist())


@default_deserialization_registry.register(StructureFamily.ragged, "application/json")
def from_json(
    contents: str | bytes,
    dtype: type,
    offsets: list[list[int]],
    shape: tuple[int | None, ...],
) -> ragged.array:
    lists_of_lists = orjson.loads(contents)
    if all(shape) and not any(offsets):
        # No raggedness, but array is not strictly N-D. Map to numpy array first.
        # Otherwise, it will infer an offset array of type='x0 * Any * ... * Any * dtype'
        # rather than a simple numpy array of type='x0 * x1 * ... * xN * dtype'.
        return ragged.array(np.array(lists_of_lists, dtype=dtype))
    return ragged.array(lists_of_lists, dtype=dtype)


def to_flattened_array(array: ragged.array) -> np.ndarray:
    content = array._impl.layout  # noqa: SLF001
    while isinstance(content, awkward.contents.ListOffsetArray):
        content = content.content
    return awkward.to_numpy(content)


@default_serialization_registry.register(
    StructureFamily.ragged, "application/octet-stream"
)
def to_flattened_octet_stream(
    mimetype: str, array: ragged.array, metadata: dict  # noqa: ARG001
) -> bytes:
    return np.asarray(to_flattened_array(array)).tobytes()


def from_flattened_array(
    array: np.ndarray,
    dtype: type,
    offsets: list[list[int]],
    shape: tuple[int | None, ...],
) -> ragged.array:
    if all(shape) and not any(offsets):
        # No raggedness, but need to reshape the flat array
        return ragged.array(array.reshape(shape), dtype=dtype)

    def rebuild(offsets: list[list[int]]) -> awkward.contents.Content:
        nonlocal array
        if not offsets:
            return awkward.contents.NumpyArray(array.tolist())
        return awkward.contents.ListOffsetArray(
            offsets=awkward.index.Index(offsets[0]), content=rebuild(offsets[1:])
        )

    return ragged.array(rebuild(offsets), dtype=dtype)


@default_deserialization_registry.register(
    StructureFamily.ragged, "application/octet-stream"
)
def from_flattened_octet_stream(
    buffer: bytes, dtype: type, offsets: list[list[int]], shape: tuple[int | None, ...]
) -> ragged.array:
    return from_flattened_array(
        np.frombuffer(buffer, dtype=dtype), dtype, offsets, shape
    )


if modules_available("pyarrow"):

    @default_serialization_registry.register(
        StructureFamily.ragged, APACHE_ARROW_FILE_MIME_TYPE
    )
    def to_arrow(mimetype: str, array: ragged.array, metadata: dict):
        components = awkward.to_buffers(array._impl)  # noqa: SLF001
        return awkward_serialization.to_arrow(mimetype, components, metadata)

    @default_serialization_registry.register(StructureFamily.ragged, PARQUET_MIMETYPE)
    def to_parquet(mimetype: str, array: ragged.array, metadata: dict):
        components = awkward.to_buffers(array._impl)  # noqa: SLF001
        return awkward_serialization.to_parquet(mimetype, components, metadata)
