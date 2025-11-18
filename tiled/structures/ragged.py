from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Dict, List, SupportsInt, Tuple, Union

import awkward
import numpy as np
import ragged

from tiled.structures.array import ArrayStructure, BuiltinDtype, StructDtype


@dataclass(kw_only=True)
class RaggedStructure(ArrayStructure):
    shape: Tuple[Union[int, None], ...]  # type: ignore[reportIncompatibleVariableOverride]
    offsets: List[List[int]]

    @classmethod
    def from_array(cls, array, shape=None, chunks=None, dims=None) -> "RaggedStructure":
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

        return cls(
            data_type=data_type,
            chunks=chunks,
            shape=shape,
            dims=dims,
            resizable=False,
            offsets=offsets,
        )

    @classmethod
    def from_json(cls, structure: dict) -> "RaggedStructure":
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
        )

    @property
    def npartitions(self) -> int:
        return 1

    @property
    def form(self) -> Dict[str, Any]:
        def build(depth: int):
            if depth:
                return {
                    "class": "NumpyArray",
                    "primitive": self.data_type.to_numpy_dtype().name,
                }
            return {
                "class": "ListOffsetArray",
                "offsets": "i64",
                "content": build(depth - 1),
            }

        return build(len(self.offsets))
