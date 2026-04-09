from collections.abc import Iterable
from dataclasses import dataclass

from tiled.structures.root import Structure


@dataclass
class ContainerStructure(Structure):
    keys: Iterable[str]
