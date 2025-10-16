from collections.abc import Mapping
from typing import Generic

from tiled.adapters.core import A, Adapter
from tiled.structures.container import ContainerStructure
from tiled.structures.core import StructureFamily


class ContainerAdapter(Adapter[ContainerStructure], Mapping[str, A], Generic[A]):
    structure_family: StructureFamily = StructureFamily.container
