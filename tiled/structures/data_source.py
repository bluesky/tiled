import dataclasses
import enum
from pathlib import Path
from typing import Generic, List, Optional, TypeVar, Union
from urllib.parse import urlparse

from ..utils import ensure_uri
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


@dataclasses.dataclass
class Storage:
    filesystem: Optional[str] = None
    sql: Optional[str] = None

    def __post_init__(self):
        if self.filesystem is not None:
            self.filesystem = ensure_uri(self.filesystem)
        if self.sql is not None:
            self.sql = ensure_uri(self.sql)

    @classmethod
    def from_path(cls, path: Union[str, Path]):
        # Interpret input as a filesystem path or 'file:' URI.
        filesystem_storage = ensure_uri(str(path))
        if not urlparse(filesystem_storage).scheme == "file":
            raise ValueError(f"Could not parse as filepath: {path}")
        return cls(filesystem=filesystem_storage)

    def get(self, key: str) -> str:
        value = getattr(self, key)
        if not value:
            raise RuntimeError(
                f"Adapter requested {key} storage but none is configured."
            )
        return value
