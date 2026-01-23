from __future__ import annotations

import io
import zipfile
from collections.abc import Iterable

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
from tiled.structures.ragged import (
    OffsetArrayType,
    RaggedStructure,
    StartAndStopArraysType,
)
from tiled.utils import modules_available, safe_json_dump


@default_serialization_registry.register(StructureFamily.ragged, "application/json")
def to_json(
    mimetype: str,
    array: ragged.array,
    metadata: dict,  # noqa: ARG001
) -> bytes:
    return safe_json_dump(array.tolist())


@default_deserialization_registry.register(StructureFamily.ragged, "application/json")
def from_json(
    contents: str | bytes,
    dtype: type,
    offsets: list[OffsetArrayType | StartAndStopArraysType],
    shape: tuple[int | None, ...],
) -> ragged.array:
    lists_of_lists = orjson.loads(contents)
    if all(shape) and not any(offsets):
        # No raggedness, but array is not strictly N-D. Map to numpy array first.
        # Otherwise, it will infer an offset array of type='x0 * Any * ... * Any * dtype'
        # rather than a simple numpy array of type='x0 * x1 * ... * xN * dtype'.
        return ragged.array(np.array(lists_of_lists, dtype=dtype))
    return ragged.array(lists_of_lists, dtype=dtype)


def to_numpy_array(array: ragged.array) -> np.ndarray:
    content = array._impl  # noqa: SLF001
    # if content is already a numpy array, return it directly
    if isinstance(content, np.ndarray):
        return content

    # strip off layers to get to underlying flat or rectilinear array
    content = content.layout
    while isinstance(
        content, (awkward.contents.ListOffsetArray, awkward.contents.ListArray)
    ):
        content = content.content

    return awkward.to_numpy(content)
    # NOTE: using awkward.flatten(...) here won't work, as it would flatten
    # any regular-shaped NumpyArray content.


@default_serialization_registry.register(
    StructureFamily.ragged, "application/octet-stream"
)
def to_numpy_octet_stream(
    mimetype: str,
    array: ragged.array,
    metadata: dict,  # noqa: ARG001
) -> bytes:
    return np.asarray(to_numpy_array(array)).tobytes()


def from_numpy_array(
    array: np.ndarray,
    dtype: type,
    offsets: list[OffsetArrayType | StartAndStopArraysType],
    shape: tuple[int | None, ...] | None = None,
) -> ragged.array:
    if shape and all(shape) and not any(offsets):
        # No raggedness, but need to reshape the flat array
        return ragged.array(array.reshape(shape), dtype=dtype)

    def rebuild(
        offsets: list[OffsetArrayType | StartAndStopArraysType],
    ) -> awkward.contents.Content:
        nonlocal array

        if offsets:
            indices, *offsets = offsets
            # if the indices are a pair of arrays, we must create a ListArray.
            # this is needed when slicing over top of a ListOffsetArray.
            if (
                len(indices) == 2
                and isinstance(indices[0], Iterable)
                and isinstance(indices[1], Iterable)
            ):
                return awkward.contents.ListArray(
                    starts=awkward.index.Index(indices[0]),
                    stops=awkward.index.Index(indices[1]),
                    content=rebuild(offsets),
                )

            return awkward.contents.ListOffsetArray(
                offsets=awkward.index.Index(indices), content=rebuild(offsets)
            )
        return awkward.contents.NumpyArray(array)

    return ragged.array(rebuild(offsets), dtype=dtype)


@default_deserialization_registry.register(
    StructureFamily.ragged, "application/octet-stream"
)
def from_numpy_octet_stream(
    buffer: bytes,
    dtype: type,
    offsets: list[OffsetArrayType | StartAndStopArraysType],
    shape: tuple[int | None, ...] | None,
) -> ragged.array:
    return from_numpy_array(np.frombuffer(buffer, dtype=dtype), dtype, offsets, shape)


@default_serialization_registry.register(
    StructureFamily.ragged, media_type="application/zip"
)
def to_zipped_buffers(
    mimetype: str,
    array: ragged.array,
    metadata: dict,  # noqa: ARG001
) -> bytes:
    file = io.BytesIO()
    with zipfile.ZipFile(file, "w", compresslevel=zipfile.ZIP_STORED) as fzip:
        fzip.writestr("shape", safe_json_dump(array.shape))
        fzip.writestr(
            "offsets", safe_json_dump(RaggedStructure.from_array(array).offsets)
        )
        fzip.writestr("data", to_numpy_array(array).tobytes())
    return file.getvalue()


@default_deserialization_registry.register(
    StructureFamily.ragged, media_type="application/zip"
)
def from_zipped_buffers(buffer: bytes, dtype: type) -> ragged.array:
    file = io.BytesIO(buffer)
    with zipfile.ZipFile(file, "r") as fzip:
        shape = orjson.loads(fzip.read("shape"))
        offsets = orjson.loads(fzip.read("offsets"))
        data = np.frombuffer(fzip.read("data"), dtype=dtype)
    return from_numpy_array(data, dtype=dtype, offsets=offsets, shape=shape)


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
