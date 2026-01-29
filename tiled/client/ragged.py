from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, urlparse

import numpy as np
import ragged

from tiled.client.base import BaseClient
from tiled.client.utils import (
    export_util,
    handle_error,
    params_from_slice,
    retry_context,
)
from tiled.ndslice import NDSlice
from tiled.serialization.ragged import (
    from_numpy_octet_stream,
    from_zipped_buffers,
    to_numpy_octet_stream,
)
from tiled.structures.ragged import RaggedStructure, make_ragged_array

if TYPE_CHECKING:
    import awkward as ak


class RaggedClient(BaseClient):
    def write(self, array: ragged.array | ak.Array | Iterable[Iterable]):
        array = make_ragged_array(array)
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

    # def write_block(self, block: int, array: ragged.array | ak.Array | list[list]):
    #     # TODO: investigate
    #     raise NotImplementedError

    def read(self, slice: NDSlice | None = None) -> ragged.array:
        url_path = self.item["links"]["full"]
        url_params: dict[str, Any] = {**parse_qs(urlparse(url_path).query)}

        if isinstance(slice, NDSlice):
            url_params["slice"] = slice.to_numpy_str()
            mimetype = "application/zip"
        else:
            mimetype = "application/octet-stream"

        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": mimetype},
                        params=url_params,
                    ),
                ).read()
        if mimetype == "application/zip":
            return from_zipped_buffers(
                buffer=content,
                dtype=self.dtype,
            )
        return from_numpy_octet_stream(
            buffer=content,
            dtype=self.dtype,
            offsets=self.offsets,
            shape=self.shape,
        )

    # def read_block(self, block: int, slice: NDSlice | None = None) -> ragged.array:
    #     # TODO: investigate
    #     raise NotImplementedError

    def __getitem__(
        self, _slice: NDSlice
    ) -> ragged.array:  # this is true even when slicing to return a single item
        # TODO: should we be smarter, and return the scalar rather a singular array
        if isinstance(_slice, tuple):
            _slice = NDSlice(*_slice)
        if not isinstance(_slice, NDSlice):
            _slice = NDSlice(_slice)
        return self.read(slice=_slice)

    def export(
        self,
        filepath: str | Path,
        *,
        slice: NDSlice | None = None,
        format: str | None = None,
    ):
        params = params_from_slice(slice)
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"]["full"],
            params=params,
        )

    @property
    def dims(self) -> list[str] | None:
        structure = cast("RaggedStructure", self.structure())
        return structure.dims

    @property
    def shape(self):
        structure = cast("RaggedStructure", self.structure())
        return structure.shape

    @property
    def offsets(self):
        structure = cast("RaggedStructure", self.structure())
        return structure.offsets

    @property
    def size(self) -> int:
        structure = cast("RaggedStructure", self.structure())
        return structure.size

    @property
    def dtype(self) -> np.dtype:
        structure = cast("RaggedStructure", self.structure())
        return structure.data_type.to_numpy_dtype()

    @property
    def nbytes(self) -> int:
        return self.size * self.dtype.itemsize

    # @property
    # def chunks(self):
    # """The structure of chunks for efficient retrieval."""
    #     structure = cast("RaggedStructure", self.structure())
    #     return structure.chunks

    @property
    def ndim(self) -> int:
        return len(self.shape)

    def __repr__(self):
        attrs = {
            "shape": self.shape,
            "size": self.size,
            "dtype": self.dtype,
        }
        if self.dims:
            attrs["dims"] = self.dims
        return (
            f"<{type(self).__name__}"
            + "".join(f" {k}={v}" for k, v in attrs.items())
            + ">"
        )
