import enum
from dataclasses import dataclass
from typing import ClassVar, Optional, Tuple, Union


class SparseLayout(str, enum.Enum):
    # Only COO is currently supported, but this lays a path
    # for adding other layouts like CSC, CSR, etc. in the future.
    COO = "COO"


@dataclass
class COOStructure:
    layout: ClassVar[SparseLayout] = SparseLayout.COO
    chunks: Tuple[Tuple[int, ...], ...]  # tuple-of-tuples-of-ints like ((3,), (3,))
    shape: Tuple[int, ...]  # tuple of ints like (3, 3)
    dims: Optional[Tuple[str, ...]] = None  # None or tuple of names like ("x", "y")
    resizable: Union[bool, Tuple[bool, ...]] = False
    # TODO Include fill_value?

    @classmethod
    def from_json(cls, structure):
        return cls(
            chunks=structure["chunks"],
            shape=structure["shape"],
            dims=structure["dims"],
            resizable=structure["resizable"],
        )


# This may be extended to a Union of structures if more are added.
SparseStructure = COOStructure
