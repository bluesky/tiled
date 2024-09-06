import enum
from dataclasses import dataclass
from typing import Optional, Tuple, Union
from .array import BuiltinDtype, StructDtype


class SparseLayout(str, enum.Enum):
    # Only COO is currently supported, but this lays a path
    # for adding other layouts like CSC, CSR, etc. in the future.
    COO = "COO"


@dataclass
class COOStructure:
    chunks: Tuple[Tuple[int, ...], ...]  # tuple-of-tuples-of-ints like ((3,), (3,))
    shape: Tuple[int, ...]  # tuple of ints like (3, 3)
    data_type: Optional[Union[BuiltinDtype, StructDtype]] = None
    dims: Optional[Tuple[str, ...]] = None  # None or tuple of names like ("x", "y")
    resizable: Union[bool, Tuple[bool, ...]] = False
    layout: SparseLayout = SparseLayout.COO
    # TODO Include fill_value?

    @classmethod
    def from_json(cls, structure):
        data_type = structure.get("data_type", None)
        if data_type is not None and "fields" in data_type:
            data_type = StructDtype.from_json(data_type)
        else:
            data_type = BuiltinDtype.from_json(data_type)
        return cls(
            data_type=data_type,
            chunks=tuple(map(tuple, structure["chunks"])),
            shape=tuple(structure["shape"]),
            dims=structure["dims"],
            resizable=structure.get("resizable", False),
        )


# This may be extended to a Union of structures if more are added.
SparseStructure = COOStructure
