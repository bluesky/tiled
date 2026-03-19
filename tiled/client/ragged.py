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
from tiled.serialization.ragged import from_zipped_buffers, to_zipped_buffers
from tiled.structures.ragged import RaggedStructure, make_ragged_array

if TYPE_CHECKING:
    import awkward as ak


class RaggedClient(BaseClient):
    def write(self, array: ragged.array | ak.Array | Iterable[Iterable]):
        """
        Write a ragged array in full.

        Parameters
        ----------
        array: ragged.array | ak.Array | Iterable[Iterable]
            The array to write. Can be a ragged.array or compatible awkward.Array,
            or any list-of-lists structure with consistent dimensions.
        """
        array = make_ragged_array(array)
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        self.item["links"]["full"],
                        content=to_zipped_buffers(
                            mimetype="application/zip",
                            array=array,
                            metadata={},
                        ),
                        headers={"Content-Type": "application/zip"},
                    )
                )

    def write_block(
        self,
        array_part: ragged.array | ak.Array | list[list] | np.ndarray,
        block: int,
        persist: bool = True,
    ):
        """
        Write a block of ragged array data.

        Parameters
        ----------
        array_part: ragged.array | ak.Array | Iterable[Iterable]
            The array to write. Can be a ragged.array or compatible awkward.Array,
            or any list-of-lists structure with consistent dimensions.
        block: int
            The block index to write to. Must be a non-negative integer less than the number of partitions.
        persist: bool, optional
            Whether to persist the changes. Default is True.
        """
        url_path = self.item["links"]["block"].format(block)
        params: dict[str, Any] = {
            **parse_qs(urlparse(url_path).query)
        }  # , **params_from_slice(slice)}
        if persist is False:
            # Extend the query only for non-default behavior.
            params["persist"] = persist
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        url_path,
                        content=to_zipped_buffers(
                            mimetype="application/zip",
                            array=array_part,
                            metadata={},
                        ),
                        headers={"Content-Type": "application/zip"},
                        params=params,
                    )
                )

    def read(self, slice: Any | None = None) -> ragged.array:
        """
        Access the entire array, or optionally a slice of it.

        Parameters
        ----------
        slice: Any, optional
            A numpy-style slice.
        """
        url_path = self.item["links"]["full"]
        url_params: dict[str, Any] = {**parse_qs(urlparse(url_path).query)}

        if slice:
            url_params.update(**params_from_slice(slice))
            # the metadata of a sliced array isn't easy to determine mathematically,
            # we should expect the server to respond with new structure information.

        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": "application/zip"},
                        params=url_params,
                    ),
                ).read()

        return from_zipped_buffers(
            buffer=content,
            dtype=self.dtype,
        )

    def read_block(self, block: int, slice: Any | None = None) -> ragged.array:
        """
        Access data for one block of the partitioned array.

        Optionally, access only a slice *within* the partition.

        Parameters
        ----------
        block: int
            The block index to read.
        slice: Any, optional
            A tuple of slice objects.
        """
        url_path = self.item["links"]["block"].format(block)
        url_params: dict[str, Any] = {**parse_qs(urlparse(url_path).query)}

        if slice:
            url_params.update(**params_from_slice(slice))

        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": "application/zip"},
                        params=url_params,
                    ),
                ).read()

        return from_zipped_buffers(
            buffer=content,
            dtype=self.dtype,
        )

    def __getitem__(self, _slice: Any) -> ragged.array:
        """
        Access the array with slicing logic.

        Parameters
        ----------
        slice: Any
            A numpy-style slice.
        """
        return self.read(slice=_slice)

    def export(
        self,
        filepath: str | Path,
        *,
        slice: Any | None = None,
        format: str | None = None,
    ):
        """
        Download data in some format and write to a file.

        Parameters
        ----------
        filepath: str or Path
            Filepath or writeable buffer.
        format : str, optional
            If format is None and `filepath` is a filepath, the format is inferred
            from the name, like 'ragged.json' implies format="application/json". The format
            may be given as a file extension ("json") or a media type ("application/json").
        slice: Any, optional
            A tuple of slice objects.
        """
        params = params_from_slice(slice)
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"]["full"],
            params=params,
        )

    @property
    def dims(self) -> tuple[str, ...] | None:
        """The dimension names of the array, if set."""
        structure = cast("RaggedStructure", self.structure())
        return structure.dims

    @property
    def shape(self) -> tuple[int | None, ...]:
        """The shape of the array, where unknown variable dimensions are given as ``None``."""
        structure = cast("RaggedStructure", self.structure())
        return structure.shape

    @property
    def size(self) -> int:
        """The total number of elements in the array."""
        structure = cast("RaggedStructure", self.structure())
        return structure.size

    @property
    def dtype(self) -> np.dtype:
        """The data type of the array."""
        structure = cast("RaggedStructure", self.structure())
        return structure.data_type.to_numpy_dtype()

    @property
    def nbytes(self) -> int:
        """The size of the array in bytes."""
        return self.size * self.dtype.itemsize

    @property
    def partitions(self) -> tuple[int, ...]:
        """The partition boundaries of the array, of form ``(0, [p1, ..., pN], rows)``."""
        structure = cast("RaggedStructure", self.structure())
        return structure.partitions

    @property
    def npartitions(self) -> int:
        """The number of partitions stored by the array."""
        structure = cast("RaggedStructure", self.structure())
        return structure.npartitions

    @property
    def ndim(self) -> int:
        """The dimensionality of the array."""
        return len(self.shape)

    def __repr__(self):
        """Return a brief representation of the ragged data accessible to the ``RaggedClient``."""
        attrs: dict[str, Any] = {
            "shape": self.shape,
            "size": self.size,
            "npartitions": self.npartitions,
            "dtype": self.dtype,
        }
        if self.dims:
            attrs["dims"] = self.dims
        return (
            f"<{type(self).__name__}"
            + "".join(f" {k}={v}" for k, v in attrs.items())
            + ">"
        )
