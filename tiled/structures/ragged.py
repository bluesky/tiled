from dataclasses import dataclass
from typing import Tuple, Union

import ragged

from tiled.structures.array import ArrayStructure, BuiltinDtype, StructDtype


@dataclass()
class RaggedStructure(ArrayStructure):
    shape: Tuple[Union[int, None], ...]  # type: ignore[reportIncompatibleVariableOverride]

    @classmethod
    def from_array(cls, array, shape=None, chunks=None, dims=None) -> "RaggedStructure":
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

        return cls(
            data_type=data_type,
            chunks=chunks,
            shape=shape,
            dims=dims,
            resizable=False,
        )
