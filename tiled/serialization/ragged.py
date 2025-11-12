from typing import List, Union

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
def to_json(mimetype: str, array: ragged.array, metadata: dict):  # noqa: ARG001
    return safe_json_dump(array.tolist())


@default_deserialization_registry.register(StructureFamily.ragged, "application/json")
def from_json(contents: Union[str, bytes]):
    return ragged.array(orjson.loads(contents))


@default_serialization_registry.register(
    StructureFamily.ragged, "application/octet-stream"
)
def to_flattened_octet_stream(mimetype: str, array: ragged.array, metadata: dict):
    content = array._impl.layout  # noqa: SLF001
    while isinstance(content, awkward.contents.ListOffsetArray):
        content = content.content
    return np.asarray(awkward.to_numpy(content)).tobytes()


@default_deserialization_registry.register(
    StructureFamily.ragged, "application/octet-stream"
)
def from_flattened_octet_stream(buffer, dtype: type, offsets: List[List[int]]):
    # return np.frombuffer(buffer, dtype=dtype)
    def rebuild(offsets: List[List[int]], data: np.ndarray) -> awkward.contents.Content:
        if not offsets:
            return awkward.contents.NumpyArray(data)
        return awkward.contents.ListOffsetArray(
            offsets=awkward.index.Index(offsets[0]),
            content=rebuild(offsets[1:], data),
        )

    data = np.frombuffer(buffer, dtype=dtype)
    return ragged.array(rebuild(offsets, data), dtype=dtype)


# @default_serialization_registry.register(StructureFamily.ragged, "application/zip")
# def to_zipped_buffers(mimetype: str, array: ragged.array, metadata: dict):
#     packed = awkward.to_packed(array._impl)  # noqa: SLF001
#     components = awkward.to_buffers(packed)
#     return awkward_serialization.to_zipped_buffers(mimetype, components, metadata)


# @default_deserialization_registry.register(StructureFamily.ragged, "application/zip")
# def from_zipped_buffers(buffer: bytes, form: dict, length: int):
#     # this should return the container dict immediately, to be used by `AwkwardBuffersAdapter`.
#     return awkward_serialization.from_zipped_buffers(buffer, form, length)


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
