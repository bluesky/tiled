from abc import ABC, abstractmethod
from collections.abc import Set
from typing import Generic, List, Optional, TypeVar

from tiled.storage import Storage
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.root import Structure
from tiled.type_aliases import JSON

S = TypeVar("S", bound=Structure)


class Adapter(ABC, Generic[S]):
    def __init__(
        self,
        structure: S,
        supported_storage: Optional[Set[type[Storage]]] = None,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ):
        self._structure = structure
        self._supported_storage = supported_storage or set()
        self._metadata = metadata or {}
        self._specs = specs or []

    @property
    def metadata(self) -> JSON:
        return self._metadata

    @property
    def specs(self) -> List[Spec]:
        return self._specs

    @property
    def structure(self) -> S:
        return self._structure

    @classmethod
    @abstractmethod
    def structure_family(cls) -> StructureFamily:
        ...
