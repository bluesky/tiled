from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from tiled.ndslice import NDSlice

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import awkward
import numpy as np
import ragged
from awkward.contents import (
    EmptyArray,
    ListArray,
    ListOffsetArray,
    NumpyArray,
    RegularArray,
)

from tiled.structures.array import ArrayStructure, BuiltinDtype, StructDtype

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


@dataclass(kw_only=True)
class RaggedStructure(ArrayStructure):
    shape: tuple[int | None, ...]  # type: ignore[reportIncompatibleVariableOverride]
    offsets: list[list[int]]
    size: int

    @staticmethod
    def make_ragged_array(array: Iterable) -> ragged.array:
        if isinstance(array, ragged.array):
            return array
        if isinstance(array, np.ndarray):
            return ragged.array(awkward.from_numpy(array))
        if isinstance(array, awkward.Array) or hasattr(array, "__dlpack_device__"):
            return ragged.array(array)
        if hasattr(array, "tolist"):
            return ragged.array(array.tolist())
        return ragged.array(list(array))

    @classmethod
    def from_array(
        cls,
        array: Iterable,
        shape: tuple[int | None, ...] | None = None,
        chunks: tuple[str, ...] | None = None,
        dims: int | None = None,
    ) -> Self:
        array = cls.make_ragged_array(array)

        if shape is None:
            shape = array.shape
        if chunks is None:
            chunks = ("auto",) * len(shape)

        if array.dtype.fields is not None:
            data_type = StructDtype.from_numpy_dtype(array.dtype)
        else:
            data_type = BuiltinDtype.from_numpy_dtype(array.dtype)

        offsets = []

        content = array._impl  # noqa: SLF001
        if hasattr(content, "layout"):
            content = content.layout

        while isinstance(content, (ListOffsetArray, ListArray)):
            if isinstance(content, ListOffsetArray):
                offsets.append(np.array(content.offsets).tolist())
            if isinstance(content, ListArray):
                offsets.append(np.array(content.to_ListOffsetArray64().offsets).tolist())
            content = content.content

        size = int(array.size)  # should never not be an int

        return cls(
            data_type=data_type,
            chunks=chunks,
            shape=shape,
            dims=dims,
            resizable=False,
            offsets=offsets,
            size=size,
        )

    @classmethod
    def from_json(cls, structure: Mapping[str, Any]) -> Self:
        if "fields" in structure["data_type"]:
            data_type = StructDtype.from_json(structure["data_type"])
        else:
            data_type = BuiltinDtype.from_json(structure["data_type"])
        dims = structure["dims"]
        if dims is not None:
            dims = tuple(dims)
        return cls(
            data_type=data_type,
            chunks=tuple(map(tuple, structure["chunks"])),
            shape=tuple(structure["shape"]),
            dims=dims,
            resizable=structure.get("resizable", False),
            offsets=structure.get("offsets", []),
            size=structure["size"],
        )

    @property
    def npartitions(self) -> int:
        return 1

    @property
    def form(self) -> dict[str, Any]:
        def build(depth: int):
            if depth <= 0:
                # TODO: Handle EmptyArray, e.g. ragged.array([[], []])
                return {
                    "class": "NumpyArray",
                    "primitive": self.data_type.to_numpy_dtype().name,
                    "form_key": f"node{len(self.offsets) - depth}",
                }
            return {
                "class": "ListOffsetArray",
                "offsets": "i64",
                "content": build(depth - 1),
                "form_key": f"node{len(self.offsets) - depth}",
            }

        return build(len(self.offsets))

    def shape_from_slice(self, _slice: NDSlice) -> tuple[int | None, ...]:
        new_shape: list[int | None] = []
        for dim, s in enumerate(_slice):
            if dim >= len(self.shape):
                break
            dim_size = self.shape[dim]
            if isinstance(s, int):
                continue
            if isinstance(s, slice):
                start, stop, step = s.indices(dim_size or sys.maxsize)
                length = (stop - start + (step - 1)) // step
                new_shape.append(length)
            else:
                raise NotImplementedError(
                    "Only integer and slice indexing are supported for RaggedStructure"
                )

        return tuple(new_shape)
