import enum
from dataclasses import dataclass
from typing import ClassVar, Optional, Tuple, Union

from .array import ArrayStructure


class SparseLayout(str, enum.Enum):
    # Only COO is currently supported, but this lays a path
    # for adding other layouts like CSC, CSR, etc. in the future.
    COO = "COO"


@dataclass
class COOStructure:
    layout: ClassVar[SparseLayout] = SparseLayout.COO
    coords: ArrayStructure
    data: ArrayStructure
    shape: Tuple[int, ...]  # tuple of ints like (3, 3)
    dims: Optional[Tuple[str, ...]] = None  # None or tuple of names like ("x", "y")
    resizable: Union[bool, Tuple[bool, ...]] = False

    @classmethod
    def from_json(cls, structure):
        return cls(
            coords=ArrayStructure.from_json(structure["coords"]),
            data=ArrayStructure.from_json(structure["data"]),
            chunks=structure["chunks"],
            shape=structure["shape"],
            dims=structure["dims"],
            resizable=structure["resizable"],
        )


# This may be extended to a Union of structures if more are added.
SparseStructure = COOStructure
