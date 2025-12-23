from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import numpy as np
import ragged

from tiled.adapters.core import Adapter
from tiled.adapters.utils import init_adapter_from_catalog
from tiled.catalog.orm import Node
from tiled.ndslice import NDSlice
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.data_source import DataSource
from tiled.structures.ragged import RaggedStructure

if TYPE_CHECKING:
    from collections.abc import Iterable

    import awkward
    from numpy.typing import NDArray

    from tiled.type_aliases import JSON


class RaggedAdapter(Adapter[RaggedStructure]):
    structure_family = StructureFamily.ragged

    def __init__(
        self,
        array: ragged.array | None,
        structure: RaggedStructure,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
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
    def from_catalog(
        cls,
        data_source: DataSource[RaggedStructure],
        node: Node,
        /,
        **kwargs: Any | None,
    ) -> Self:
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)

    @classmethod
    def from_array(
        cls,
        array: ragged.array | awkward.Array | NDArray[Any] | Iterable[Iterable[Any]],
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> Self:
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
        if self._array is None:
            raise NotImplementedError
        # _array[...] requires an actual tuple, not just a subclass of tuple
        return self._array[tuple(slice)] if slice else self._array

    def write(
        self,
        array: ragged.array,
    ) -> None:
        raise NotImplementedError

    def metadata(self) -> JSON:
        return self._metadata

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure})"

    def structure(self) -> RaggedStructure:
        return self._structure
