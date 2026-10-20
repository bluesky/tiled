from __future__ import annotations

from pathlib import Path
from typing import Any, Union

import numpy as np
import ragged

from ..structures.ragged import RaggedCompatibleType, make_ragged_array
from ._protocol.ragged import (
    PATCH_CONFLICT_STATUS_CODES,
    RaggedReadRecipe,
    draft_patch_request,
    draft_write_block_request,
    draft_write_request,
    parse_patch_response,
    patch_conflict_exception,
)
from .base import BaseClient
from .utils import export_util, handle_error, params_from_slice, retry_context


class RaggedClient(BaseClient):
    def write(self, array: RaggedCompatibleType, persist=True):
        """Write a ragged array in full.

        Parameters
        ----------
        array: RaggedCompatibleType
            The array to write. Can be a ragged.array or compatible awkward.Array,
            or any list-of-lists structure with consistent dimensions.
        persist: bool
            Whether to persist the changes on server storage. If False, the update is still
            streamed to subscribed listeners, but the server may choose not to save the changes.
        """
        draft = draft_write_request(self.item["links"]["full"], array, persist)
        for attempt in retry_context():
            with attempt:
                handle_error(self._send(draft))

    def write_block(
        self,
        array: RaggedCompatibleType,
        block: Union[int, tuple[int, ...]],
        persist=True,
    ):
        """Write a block of ragged array data.

        Parameters
        ----------
        array: RaggedCompatibleType
            The array to write. Can be a ragged.array or compatible awkward.Array,
            or any list-of-lists structure with consistent dimensions.
        block: int or tuple[int, ...]
            The block to write to. Must be a non-negative integer less than the number of
            partitions or a tuple of such integers, one for each partitioned dimension.
        persist: bool
            Whether to persist the changes on server storage. If False, the update is still
            streamed to subscribed listeners, but the server may choose not to save the changes.
        """
        draft = draft_write_block_request(
            self.item["links"]["block"], block, array, persist
        )
        for attempt in retry_context():
            with attempt:
                handle_error(self._send(draft))

    def patch(
        self,
        array: RaggedCompatibleType,
        offset: Union[int, tuple[int, ...]],
        extend=False,
        persist=True,
    ):
        """Write data into a slice of an array, possibly extending the shape.

        Parameters
        ----------
        array : RaggedCompatibleType
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
            raise NotImplementedError(
                "Only extend=True and persist=True are currently supported"
            )

        offset = (offset,) if isinstance(offset, int) else offset
        if offset[0] != self.shape[0]:
            raise NotImplementedError(
                "Only appending to the end of the leftmost dimension is currently supported"
            )

        array = make_ragged_array(array)

        if array.dtype != self.dtype:
            raise ValueError(
                f"Data given to patch has dtype {array.dtype} which does not "
                f"match the dtype of this array {self.dtype}."
            )

        draft = draft_patch_request(
            self.item["links"]["full"], array, offset, extend, persist
        )
        for attempt in retry_context():
            with attempt:
                response = self._send(draft)
                if response.status_code in PATCH_CONFLICT_STATUS_CODES:
                    raise patch_conflict_exception(
                        response.status_code, response.json()
                    )
                handle_error(response)
        # Update cached structure.
        self._structure = parse_patch_response(response.json(), self.structure_family)

    def read(self, slice: Any | None = None) -> ragged.array:
        """Read the entire array, or optionally a slice of it.

        Parameters
        ----------
        slice: Any, optional
            A numpy-style slice.
        """
        recipe = RaggedReadRecipe.for_full(
            self.item["links"]["full"], self.structure(), slice
        )
        for attempt in retry_context():
            with attempt:
                content = handle_error(self._send(recipe.draft)).read()
        return recipe.parse(content)

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
        """Download data in some format and write to a file.

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
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"]["full"],
            params=params_from_slice(slice),
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
