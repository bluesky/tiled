from typing import Optional, Tuple, Union

import pydantic

from ..structures.array import BuiltinDtype, Endianness, Kind, StructDtype
from ..structures.sparse import SparseLayout


class COOStructure(pydantic.BaseModel):
    shape: Tuple[int, ...]  # tuple of ints like (3, 3)
    chunks: Tuple[Tuple[int, ...], ...]  # tuple-of-tuples-of-ints like ((3,), (3,))
    data_type: Optional[Union[BuiltinDtype, StructDtype]] = None
    coord_data_type: Optional[BuiltinDtype] = BuiltinDtype(
        Endianness("little"), Kind("u"), 8
    )  # numpy 'uint' dtype
    dims: Optional[Tuple[str, ...]] = None  # None or tuple of names like ("x", "y")
    resizable: Union[bool, Tuple[bool, ...]] = False
    layout: SparseLayout = SparseLayout.COO

    model_config = pydantic.ConfigDict(extra="forbid")


# This may be extended to a Union of structures if more are added.
SparseStructure = COOStructure
