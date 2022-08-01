from typing import ClassVar, Optional, Tuple, Union

import pydantic

from ..structures.sparse import SparseLayout


class COOStructure(pydantic.BaseModel):
    layout: ClassVar[SparseLayout] = SparseLayout.COO
    shape: Tuple[int, ...]  # tuple of ints like (3, 3)
    chunks: Tuple[Tuple[int, ...], ...]  # tuple-of-tuples-of-ints like ((3,), (3,))
    dims: Optional[Tuple[str, ...]] = None  # None or tuple of names like ("x", "y")
    resizable: Union[bool, Tuple[bool, ...]] = False


# This may be extended to a Union of structures if more are added.
SparseStructure = COOStructure
