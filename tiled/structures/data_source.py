import dataclasses
import enum
from collections.abc import Mapping
from typing import Any, Generic, List, Optional, TypeVar

from tiled.structures.root import Structure

from .core import StructureFamily


class Management(str, enum.Enum):
    external = "external"
    immutable = "immutable"
    locked = "locked"
    writable = "writable"


@dataclasses.dataclass
class Asset:
    data_uri: str
    is_directory: bool
    parameter: Optional[str]
    num: Optional[int] = None
    id: Optional[int] = None


StructureT = TypeVar("StructureT", bound=Optional[Structure])


@dataclasses.dataclass
class DataSource(Generic[StructureT]):
    structure_family: StructureFamily
    structure: StructureT
    id: Optional[int] = None
    mimetype: Optional[str] = None
    parameters: dict = dataclasses.field(default_factory=dict)
    assets: List[Asset] = dataclasses.field(default_factory=list)
    management: Management = Management.writable

    @classmethod
    def from_json(cls, structure: Mapping[str, Any]) -> "DataSource":
        d = structure.copy()
        assets = [Asset(**a) for a in d.pop("assets")]
        return cls(assets=assets, **d)
