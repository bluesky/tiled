from collections.abc import Mapping
from typing import Generic, TypeVar

from tiled.adapters.core import S, Adapter
from tiled.structures.container import ContainerStructure
from tiled.structures.core import StructureFamily

class ContainerAdapter(Adapter[ContainerStructure], Mapping[str, A], Generic[A]):
    @classmethod
    def structure_family(cls) -> StructureFamily:
        return StructureFamily.container
