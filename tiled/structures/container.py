from collections.abc import Iterable

from tiled.structures.root import Structure


class ContainerStructure(Structure):
    keys: Iterable[str]
