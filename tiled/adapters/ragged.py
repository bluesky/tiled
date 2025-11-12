from collections.abc import Iterable
from typing import Any, ClassVar, List, Optional, Set, Union

import awkward
import numpy as np
import ragged
from numpy.typing import NDArray

from tiled.ndslice import NDSlice
from tiled.storage import FileStorage, Storage
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.ragged import RaggedStructure
from tiled.type_aliases import JSON


class RaggedAdapter:
    structure_family = StructureFamily.ragged
    supported_storage: ClassVar[Set[type[Storage]]] = {FileStorage}

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
        array: Union[
            ragged.array, awkward.Array, NDArray[Any], Iterable[Iterable[Any]]
        ],
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
        array = (
            ragged.array(list(array))
            if isinstance(array, np.ndarray)
            else ragged.asarray(array)
        )

        structure = RaggedStructure.from_array(array)
        return cls(
            array,
            structure,
            metadata=metadata,
            specs=specs,
        )

    def read(
        self,
        slice: NDSlice = NDSlice(...),
    ) -> ragged.array:
        """

        Parameters
        ----------
        slice :

        Returns
        -------

        """
        # _array[...] requires an actual tuple, not just a subclass of tuple
        return self._array[tuple(slice)] if slice else self._array

    def write(
        self,
        array: ragged.array,
    ) -> None:
        raise Exception

    def metadata(self) -> JSON:
        return self._metadata

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure})"

    def structure(self) -> RaggedStructure:
        return self._structure
