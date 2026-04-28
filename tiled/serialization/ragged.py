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
from tiled.utils import SerializationError, modules_available, safe_json_dump

OffsetArrayType = list[int]
"""Represents a list of offsets for ``awkward.contents.ListOffsetArray`` layouts."""
StartAndStopArraysType = tuple[list[int], list[int]]
"""Represents a pair of lists, ``[starts, stops]``, for ``awkward.contents.ListArray`` layouts.

While ``ListArray`` is convertible to ``ListOffsetArray``, we need this to retain information
when slicing and dicing ragged arrays.
"""


@default_serialization_registry.register(StructureFamily.ragged, "application/json")
def to_json(
    mimetype: str,
    array: ragged.array,
    metadata: dict,  # noqa: ARG001
) -> bytes:
    msg = "Cannot serialize scalar value to JSON."
    if array.ndim == 0:
        raise SerializationError(msg)
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


def _deconstruct_ragged(
    array: ragged.array,
) -> tuple[
    np.ndarray, list[OffsetArrayType | StartAndStopArraysType], tuple[int | None, ...]
]:
    offsets: list[OffsetArrayType | StartAndStopArraysType] = []
    content = array._impl  # noqa: SLF001
    if hasattr(content, "layout"):
        content = content.layout

    while isinstance(
        content, (awkward.contents.ListOffsetArray, awkward.contents.ListArray)
    ):
        if isinstance(content, awkward.contents.ListOffsetArray):
            offsets.append(np.array(content.offsets).tolist())
        else:
            start = np.array(content.starts).tolist()
            stop = np.array(content.stops).tolist()
            offsets.append([start, stop])
        content = content.content

    return awkward.to_numpy(content), offsets, array.shape
    # NOTE: using awkward.flatten(...) here won't work, as it would flatten
    # any regular-shaped NumpyArray content.


def _construct_ragged(
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


@default_serialization_registry.register(
    StructureFamily.ragged, media_type="application/zip"
)
def to_zipped_buffers(
    mimetype: str,
    array: ragged.array,
    metadata: dict,  # noqa: ARG001
) -> bytes:
    data, offsets, shape = _deconstruct_ragged(array)
    file = io.BytesIO()
    with zipfile.ZipFile(file, "w", compresslevel=zipfile.ZIP_STORED) as fzip:
        fzip.writestr("data", data.tobytes())
        fzip.writestr("offsets", safe_json_dump(offsets))
        fzip.writestr("shape", safe_json_dump(shape))
    return file.getvalue()


@default_deserialization_registry.register(
    StructureFamily.ragged, media_type="application/zip"
)
def from_zipped_buffers(
    buffer: bytes,
    dtype: type,
    *unused,  # match interface of other deserializers; shape and offsets are known from zipped contents
) -> ragged.array:
    file = io.BytesIO(buffer)
    with zipfile.ZipFile(file, "r") as fzip:
        data = np.frombuffer(fzip.read("data"), dtype=dtype)
        offsets = orjson.loads(fzip.read("offsets"))
        shape = orjson.loads(fzip.read("shape"))
    return _construct_ragged(data, dtype=dtype, offsets=offsets, shape=shape)


if modules_available("pyarrow"):

    @default_serialization_registry.register(
        StructureFamily.ragged, APACHE_ARROW_FILE_MIME_TYPE
    )
    def to_arrow(mimetype: str, array: ragged.array, metadata: dict):
        msg = "Cannot serialize scalar value to Arrow."
        if array.ndim == 0:
            raise SerializationError(msg)
        components = awkward.to_buffers(array._impl)  # noqa: SLF001
        return awkward_serialization.to_arrow(mimetype, components, metadata)

    @default_serialization_registry.register(StructureFamily.ragged, PARQUET_MIMETYPE)
    def to_parquet(mimetype: str, array: ragged.array, metadata: dict):
        msg = "Cannot serialize scalar value to Parquet."
        if array.ndim == 0:
            raise SerializationError(msg)
        components = awkward.to_buffers(array._impl)  # noqa: SLF001
        return awkward_serialization.to_parquet(mimetype, components, metadata)
