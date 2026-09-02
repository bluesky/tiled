import dataclasses
import enum
from collections.abc import Mapping
from typing import Any, Generic, List, Optional, TypeVar

from tiled.structures.root import Structure

from ..utils import filter_known_kwargs
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
    size: Optional[int] = None

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> "Asset":
        return cls(**filter_known_kwargs(cls, data))


StructureT = TypeVar("StructureT", bound=Optional[Structure])


@dataclasses.dataclass
class DataSource(Generic[StructureT]):
    structure_family: StructureFamily
    structure: StructureT
    id: Optional[int] = None
    mimetype: Optional[str] = None
    parameters: dict = dataclasses.field(default_factory=dict)
    properties: dict = dataclasses.field(default_factory=dict)
    assets: List[Asset] = dataclasses.field(default_factory=list)
    management: Management = Management.writable

    @classmethod
    def from_json(cls, structure: Mapping[str, Any]) -> "DataSource":
        d = dict(structure)
        assets = [Asset.from_json(a) for a in d.pop("assets", [])]
        return cls(assets=assets, **filter_known_kwargs(cls, d))
