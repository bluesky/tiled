from typing import Any, List, Optional, Set

import ragged
from numpy.typing import NDArray

from tiled.ndslice import NDSlice
from tiled.storage import Storage
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.ragged import RaggedStructure
from tiled.type_aliases import JSON


class RaggedAdapter:
    structure_family = StructureFamily.ragged
    supported_storage: Set[type[Storage]] = set()

    def __init__(
        self,
        array: ragged.array,
        structure: RaggedStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        """

        Parameters
        ----------
        array :
        structure :
        metadata :
        specs :
        """
        self._array = array
        self._structure = structure
        self._metadata = metadata or {}
        self.specs = list(specs or [])

    @classmethod
    def from_array(
        cls,
        array: NDArray[Any],
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> "RaggedAdapter":
        """

        Parameters
        ----------
        array :
        metadata :
        specs :

        Returns
        -------

        """
        structure = RaggedStructure.from_array(array)
        return cls(
            array,
            structure,
            metadata=metadata,
            specs=specs,
        )

    def metadata(self) -> JSON:
        return self._metadata

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure})"

    def read(self, slice: NDSlice = NDSlice(...)) -> ragged.array:
        return self._array[tuple(slice)] if slice else self._array
