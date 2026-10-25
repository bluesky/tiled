from __future__ import annotations

from pathlib import Path
from typing import Any, Union

import numpy as np
import ragged

from ..structures.core import structure_from_json
from ..structures.ragged import RaggedCompatibleType, make_ragged_array
from ._protocol.ragged import (
    RaggedReadRecipe,
    draft_request_patch_ragged_full,
    draft_request_write_ragged_block,
    draft_request_write_ragged_full,
)
from .base import BaseClient
from .utils import export_util, params_from_slice, translating_patch_conflicts


class CommonRaggedClient:
    """Pure ragged-array accessors and glue shared by the sync and async clients.

    A mixin over `CommonBaseClient`: everything here reads the cached
    structure; nothing performs I/O.
    """

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
        """Return a brief representation of the ragged data accessible to the client."""
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

    # -- Glue between instance state and the protocol layer ------------------
    # Pure methods that bind this client's state (item, structure) to the
    # builders and parsers in `tiled.client._protocol`. The sync and async
    # shells share them; only the sends in between differ.

    def _draft_request_write(self, array, persist):
        "Draft the request that overwrites the array in full."
        return draft_request_write_ragged_full(
            self.item["links"]["full"], array, persist
        )

    def _draft_request_write_block(self, array, block, persist):
        "Draft the request that overwrites one block of the array."
        return draft_request_write_ragged_block(
            self.item["links"]["block"], block, array, persist
        )

    def _read_recipe(self, slice=None):
        "A recipe that reads the whole array or a slice of it."
        return RaggedReadRecipe.for_full(
            self.item["links"]["full"], self.structure(), slice
        )

    def _draft_request_patch(self, array, offset, extend, persist):
        "Draft the request that patches a slice of the array, after validation."
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
        return draft_request_patch_ragged_full(
            self.item["links"]["full"], array, offset, extend, persist
        )


class RaggedClient(CommonRaggedClient, BaseClient):
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
        self._send(self._draft_request_write(array, persist))

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
        self._send(self._draft_request_write_block(array, block, persist))

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
        draft = self._draft_request_patch(array, offset, extend, persist)
        with translating_patch_conflicts():
            response = self._send(draft)
        # Update cached structure.
        self._structure = structure_from_json(self.structure_family, response.json())

    def read(self, slice: Any | None = None) -> ragged.array:
        """Read the entire array, or optionally a slice of it.

        Parameters
        ----------
        slice: Any, optional
            A numpy-style slice.
        """
        recipe = self._read_recipe(slice)
        content = self._send(recipe.draft).read()
        return recipe.parse_response(content)

    def __getitem__(self, _slice: Any) -> ragged.array:
        """Access the array with slicing logic.

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
            self.context,
            self.item["links"]["full"],
            params=params_from_slice(slice),
        )
