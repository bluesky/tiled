from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, urlparse

import ragged

from tiled.client.base import BaseClient
from tiled.client.utils import chunks_repr, export_util, handle_error, retry_context
from tiled.ndslice import NDSlice
from tiled.serialization.ragged import (
    from_numpy_octet_stream,
    from_zipped_buffers,
    to_numpy_octet_stream,
)

if TYPE_CHECKING:
    import awkward as ak

    from tiled.structures.ragged import RaggedStructure


class RaggedClient(BaseClient):
    def write(self, array: ragged.array | ak.Array | list[list]):
        array = (
            ragged.array(array, dtype=array.dtype)
            if hasattr(array, "dtype")
            else ragged.array(array)
        )
        mimetype = "application/octet-stream"
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        self.item["links"]["full"],
                        content=to_numpy_octet_stream(
                            mimetype=mimetype,
                            array=array,
                            metadata={},
                        ),
                        headers={"Content-Type": mimetype},
                    ),
                )

    def write_block(self, block: int, array: ragged.array | ak.Array | list[list]):
        # TODO: investigate
        raise NotImplementedError

    def read(self, slice: NDSlice | None = None) -> ragged.array:
        structure = cast("RaggedStructure", self.structure())
        url_path = self.item["links"]["full"]
        url_params: dict[str, Any] = {**parse_qs(urlparse(url_path).query)}

        if isinstance(slice, NDSlice):
            url_params["slice"] = slice.to_numpy_str()
            mime = "application/zip"
        else:
            mime = "application/octet-stream"

        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": mime},
                        params=url_params,
                    ),
                ).read()
        if mime == "application/zip":
            return from_zipped_buffers(
                buffer=content,
                dtype=structure.data_type.to_numpy_dtype(),
            )
        return from_numpy_octet_stream(
            buffer=content,
            dtype=structure.data_type.to_numpy_dtype(),
            offsets=structure.offsets,
            shape=structure.shape,
        )

    def read_block(self, block: int, slice: NDSlice | None = None) -> ragged.array:
        # TODO: investigate
        raise NotImplementedError

    def __getitem__(
        self, _slice: NDSlice
    ) -> ragged.array:  # this is true even when slicing to return a single item
        # TODO: should we be smarter, and return the scalar rather a singular array
        if isinstance(_slice, tuple):
            _slice = NDSlice(*_slice)
        if not isinstance(_slice, NDSlice):
            _slice = NDSlice(_slice)
        return self.read(slice=_slice)

    def export(self, filepath, *, format=None):
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"]["full"],
            params={},
        )

    @property
    def dims(self):
        structure = cast("RaggedStructure", self.structure())
        return structure.dims

    @property
    def shape(self):
        structure = cast("RaggedStructure", self.structure())
        return structure.shape

    @property
    def size(self):
        structure = cast("RaggedStructure", self.structure())
        return structure.size

    @property
    def dtype(self):
        structure = cast("RaggedStructure", self.structure())
        return structure.data_type.to_numpy_dtype()

    @property
    def nbytes(self):
        structure = cast("RaggedStructure", self.structure())
        itemsize = structure.data_type.to_numpy_dtype().itemsize
        return structure.size * itemsize

    @property
    def chunks(self):
        structure = cast("RaggedStructure", self.structure())
        return structure.chunks

    @property
    def ndim(self):
        structure = cast("RaggedStructure", self.structure())
        return len(structure.shape)

    def __repr__(self):
        structure = cast("RaggedStructure", self.structure())
        attrs = {
            "shape": structure.shape,
            "chunks": chunks_repr(structure.chunks),
            "dtype": structure.data_type.to_numpy_dtype(),
        }
        if structure.dims:
            attrs["dims"] = structure.dims
        return (
            f"<{type(self).__name__}"
            + "".join(f" {k}={v}" for k, v in attrs.items())
            + ">"
        )
