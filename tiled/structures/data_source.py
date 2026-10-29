import dataclasses
import enum
from collections.abc import Mapping
from typing import Any, Generic, TypeVar

from tiled.structures.root import Structure

from ..utils import filter_known_kwargs
from .core import StructureFamily


class Management(enum.StrEnum):
    external = "external"
    immutable = "immutable"
    locked = "locked"
    writable = "writable"


@dataclasses.dataclass
class Asset:
    data_uri: str
    is_directory: bool
    parameter: str | None
    num: int | None = None
    id: int | None = None
    size: int | None = None

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> "Asset":
        return cls(**filter_known_kwargs(cls, data))


StructureT = TypeVar("StructureT", bound=Structure | None)


@dataclasses.dataclass
class DataSource(Generic[StructureT]):
    structure_family: StructureFamily
    structure: StructureT
    id: int | None = None
    mimetype: str | None = None
    parameters: dict = dataclasses.field(default_factory=dict)
    properties: dict = dataclasses.field(default_factory=dict)
    assets: list[Asset] = dataclasses.field(default_factory=list)
    management: Management = Management.writable

    @classmethod
    def from_json(cls, structure: Mapping[str, Any]) -> "DataSource":
        d = dict(structure)
        assets = [Asset.from_json(a) for a in d.pop("assets", [])]
        return cls(assets=assets, **filter_known_kwargs(cls, d))
