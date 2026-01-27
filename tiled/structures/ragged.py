from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import awkward
import numpy as np
import ragged
from awkward.contents import ListArray, ListOffsetArray

from tiled.structures.array import ArrayStructure, BuiltinDtype, StructDtype

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

OffsetArrayType = list[int]
"""Represents a list of offsets for ``awkward.contents.ListOffsetArray`` layouts."""
StartAndStopArraysType = tuple[list[int], list[int]]
"""Represents a pair of lists, ``[starts, stops]``, for ``awkward.contents.ListArray`` layouts.

While ``ListArray`` is convertible to ``ListOffsetArray``, we need this to retain information
when slicing and dicing ragged arrays.
"""


@dataclass(kw_only=True)
class RaggedStructure(ArrayStructure):
    shape: tuple[int | None, ...]  # type: ignore[reportIncompatibleVariableOverride]
    offsets: list[OffsetArrayType | StartAndStopArraysType]
    size: int

    @staticmethod
    def make_ragged_array(array: Iterable) -> ragged.array:
        if isinstance(array, ragged.array):
            return array
        if isinstance(array, np.ndarray):
            if array.dtype.name == "object":
                return ragged.array([row.tolist() for row in array])
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

        offsets: list[OffsetArrayType | StartAndStopArraysType] = []

        content = array._impl  # noqa: SLF001
        if hasattr(content, "layout"):
            content = content.layout

        while isinstance(content, (ListOffsetArray, ListArray)):
            if isinstance(content, ListOffsetArray):
                offsets.append(np.array(content.offsets).tolist())
            if isinstance(content, ListArray):
                start = np.array(content.starts).tolist()
                stop = np.array(content.stops).tolist()
                offsets.append([start, stop])
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
