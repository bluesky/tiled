from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

import awkward
import numpy as np
import ragged

from tiled.structures.array import ArrayStructure, BuiltinDtype, StructDtype

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


@dataclass(kw_only=True)
class RaggedStructure(ArrayStructure):
    shape: tuple[int | None, ...]  # type: ignore[reportIncompatibleVariableOverride]
    offsets: list[list[int]]
    size: int

    @classmethod
    def from_array(
        cls,
        array: Iterable,
        shape: tuple[int | None, ...] | None = None,
        chunks: tuple[str, ...] | None = None,
        dims: int | None = None,
    ) -> Self:
        if not isinstance(array, ragged.array):
            array = (
                ragged.asarray(array.tolist())
                if hasattr(array, "tolist")
                else ragged.array(list(array))
            )

        if shape is None:
            shape = array.shape
        if chunks is None:
            chunks = ("auto",) * len(shape)

        if array.dtype.fields is not None:
            data_type = StructDtype.from_numpy_dtype(array.dtype)
        else:
            data_type = BuiltinDtype.from_numpy_dtype(array.dtype)

        content = array._impl.layout  # noqa: SLF001
        offsets = []

        while isinstance(content, awkward.contents.ListOffsetArray):
            offsets.append(np.array(content.offsets).tolist())
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
