from abc import ABC
from collections.abc import Set
from typing import Any, Generic, List, Optional, TypeVar

from tiled.storage import Storage
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.root import Structure
from tiled.type_aliases import JSON

S = TypeVar("S", bound=Structure)


class Adapter(ABC, Generic[S]):
    structure_family: StructureFamily

    def __init__(
        self,
        structure: S,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ):
        self._structure = structure
        self._metadata = metadata or {}
        self._specs = specs or []

    def metadata(self) -> JSON:
        return self._metadata

    @property
    def specs(self) -> List[Spec]:
        return self._specs

    @specs.setter
    def specs(self, value: List[Spec]) -> None:
        self._specs = value

    def structure(self) -> S:
        return self._structure

    @classmethod
    def supported_storage(cls) -> Set[type[Storage]]:
        return set()


A = TypeVar("A", bound=Adapter[Any])
