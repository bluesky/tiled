from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union, cast
from urllib.parse import parse_qs, urlparse
from ..structures.core import STRUCTURE_TYPES

import httpx

import numpy as np
import ragged

from .base import BaseClient
from .utils import (
    export_util,
    handle_error,
    params_from_slice,
    retry_context,
)
from ..ndslice import NDBlock
from ..serialization.ragged import from_zipped_buffers, to_zipped_buffers
from ..structures.ragged import RaggedStructure, make_ragged_array

if TYPE_CHECKING:
    import awkward as ak


class RaggedClient(BaseClient):
    def write(self, array: ragged.array | ak.Array | Iterable[Iterable]):
        """Write a ragged array in full.

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
                            mimetype="application/zip", array=array, metadata={}
                        ),
                        headers={"Content-Type": "application/zip"},
                    )
                )

    def write_block(
        self,
        array_part: ragged.array | ak.Array | list[list] | np.ndarray,
        block: Any,
        persist: bool = True,
    ):
        """Write a block of ragged array data.

        Parameters
        ----------
        array_part: ragged.array | ak.Array | Iterable[Iterable]
            The array to write. Can be a ragged.array or compatible awkward.Array,
            or any list-of-lists structure with consistent dimensions.
        block: NDBlock
            The block to write to. Must be a non-negative integer less than the number of partitions.
        persist: bool, optional
            Whether to persist the changes. Default is True.
        """
        if not isinstance(block, NDBlock):
            block = NDBlock(block)
        block_str = block.expand_for_shape(
            self.structure().shape_from_chunks
        ).to_numpy_str()

        url_path = self.item["links"]["block"]
        params: dict[str, Any] = {
            **parse_qs(urlparse(url_path).query),
            "block": block_str,
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
                            mimetype="application/zip", array=array_part, metadata={}
                        ),
                        headers={"Content-Type": "application/zip"},
                        params=params,
                    )
                )

    def patch(
        self,
        array: ragged.array | ak.Array | Iterable[Iterable],
        offset: Union[int, tuple[int, ...]],
        extend=False,
        persist=True,
    ):
        """Write data into a slice of an array, possibly extending the shape.

        Parameters
        ----------
        array : ragged.array | ak.Array | Iterable[Iterable]
            The data to write
        offset : int | tuple[int, ...]
            Where to place this data in the array
        extend : bool
            Extend the array shape to fit the new slice, if necessary
        persist : bool | None
            Persist the changes on server storage if True. [default behavior]
            If False, the update is still streamed to subscribed listeners.
        """
        if not extend or not persist:
            raise NotImplementedError("Only extend=True and persist=True are currently supported")

        if not isinstance(offset, int) or offset != self.shape[0]:
            raise NotImplementedError("Only appending to the end of the leftmost dimension is currently supported")

        array = make_ragged_array(array)

        if array.dtype != self.dtype:
            raise ValueError(
                f"Data given to patch has dtype {array.dtype} which does not "
                f"match the dtype of this array {self.dtype}."
            )

        if isinstance(offset, int):
            offset = (offset,)
        url_path = self.item["links"]["full"]
        params = {
            **parse_qs(urlparse(url_path).query),
            "shape": str(array.shape[0]),
            "offset": ",".join(map(str, offset)),
            "extend": bool(extend),
        }
        if persist is False:
            # Extend the query only for non-default behavior.
            params["persist"] = persist
        for attempt in retry_context():
            with attempt:
                response = self.context.http_client.patch(
                    url_path,
                    content=to_zipped_buffers(
                            mimetype="application/zip", array=array, metadata={}
                        ),
                    headers={"Content-Type": "application/zip"},
                    params=params,
                )
                if response.status_code in [
                    httpx.codes.BAD_REQUEST,
                    httpx.codes.CONFLICT,
                ]:
                    raise ValueError(
                        response.json()
                        .get("detail", "Array parameters conflict.")
                        .replace(
                            "Use ?",  # URL query param
                            "Pass keyword argument ",  # Python function argument
                        )
                    )
                handle_error(response)
        # Update cached structure.
        new_structure = response.json()
        structure_type = STRUCTURE_TYPES[self.structure_family]
        self._structure = structure_type.from_json(new_structure)

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
                    )
                ).read()

        return from_zipped_buffers(buffer=content, dtype=self.dtype)

    def read_block(self, block: Any, slice: Any | None = None) -> ragged.array:
        """
        Access data for one block of the partitioned array.

        Optionally, access only a slice *within* the partition.

        Parameters
        ----------
        block: NDBlock | int
            The block(s) to read.
        slice: Any, optional
            A tuple of slice objects.
        """
        if not isinstance(block, NDBlock):
            block = NDBlock(block)
        block_str = block.expand_for_shape(
            self.structure().shape_from_chunks
        ).to_numpy_str()

        url_path = self.item["links"]["block"]
        url_params: dict[str, Any] = {
            **parse_qs(urlparse(url_path).query),
            "block": block_str,
        }

        if slice:
            url_params.update(**params_from_slice(slice))

        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.get(
                        url_path,
                        headers={"Accept": "application/zip"},
                        params=url_params,
                    )
                ).read()

        return from_zipped_buffers(buffer=content, dtype=self.dtype)

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
        return self.structure().dims

    @property
    def shape(self) -> tuple[int | None, ...]:
        """The shape of the array, where unknown variable dimensions are given as ``None``."""
        return self.structure().shape

    @property
    def size(self) -> int:
        """The total number of elements in the array."""
        return self.structure().size

    @property
    def dtype(self) -> np.dtype:
        """The data type of the array."""
        return self.structure().data_type.to_numpy_dtype()

    @property
    def chunks(self) -> tuple[tuple[int, ...] | None, ...]:
        """The dask-like chunks of the array, where the first dimension is always
        partitioned into known integer chunks, and any variable dimensions are `None`.
        """
        return self.structure().chunks

    @property
    def chunked(self) -> bool:
        """Whether the array is chunked along any dimension."""
        return any(c is not None and len(c) > 1 for c in self.chunks)

    @property
    def ndim(self) -> int:
        """The dimensionality of the array."""
        return len(self.shape)

    def __repr__(self):
        """Return a brief representation of the ragged data accessible to the ``RaggedClient``."""
        attrs: dict[str, Any] = {
            "shape": self.shape,
            "size": self.size,
            "chunks": self.chunks,
            "dtype": self.dtype,
        }
        if self.dims:
            attrs["dims"] = self.dims
        return (
            f"<{type(self).__name__}"
            + "".join(f" {k}={v}" for k, v in attrs.items())
            + ">"
        )
