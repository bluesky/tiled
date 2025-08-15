from dataclasses import dataclass
from typing import Tuple, Union

import ragged

from tiled.structures.array import ArrayStructure, BuiltinDtype, StructDtype


@dataclass()
class RaggedStructure(ArrayStructure):
    shape: Tuple[Union[int, None], ...]  # type: ignore[reportIncompatibleVariableOverride]

    @classmethod
    def from_array(cls, array, shape=None, chunks=None, dims=None) -> "RaggedStructure":
        from dask.array.core import normalize_chunks

        # TODO: test, or implement conversion from, AwkwardArrays
        array = ragged.asarray(array)

        if shape is None:
            shape = array.shape
        if chunks is None:
            chunks = ("auto",) * len(shape)

        # TODO test chunking: I think this should default to the largest superset of "shapes"
        normalized_chunks = normalize_chunks(
            chunks,
            shape=shape,
            dtype=array.dtype,
        )
        if array.dtype.fields is not None:
            data_type = StructDtype.from_numpy_dtype(array.dtype)
        else:
            data_type = BuiltinDtype.from_numpy_dtype(array.dtype)

        return cls(
            data_type=data_type,
            chunks=normalized_chunks,
            shape=shape,
            dims=dims,
            resizable=False,
        )
