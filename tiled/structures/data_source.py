import dataclasses
import enum
from typing import Generic, List, Optional, TypeVar

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


StructureT = TypeVar("StructureT")


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
    def from_json(cls, d):
        d = d.copy()
        assets = [Asset(**a) for a in d.pop("assets")]
        return cls(assets=assets, **d)
