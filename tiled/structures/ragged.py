from dataclasses import dataclass
from typing import Tuple, Union

import ragged

from tiled.structures.array import ArrayStructure, BuiltinDtype, StructDtype


@dataclass(kw_only=True)
class RaggedStructure(ArrayStructure):
    shape: Tuple[Union[int, None], ...]  # type: ignore[reportIncompatibleVariableOverride]
    length: int
    form: dict

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

        length = array._impl.layout.length
        form = array._impl.layout.form.to_dict()

        return cls(
            data_type=data_type,
            chunks=chunks,
            shape=shape,
            dims=dims,
            resizable=False,
            length=length,
            form=form,
        )

    @classmethod
    def from_json(cls, structure):
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
            length=structure["length"],
            form=dict(structure["form"]),
        )
