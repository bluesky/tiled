import collections
import dataclasses
import enum
from typing import Any, List, Optional

from ..structures.core import BaseStructureFamily, StructureFamily


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


@dataclasses.dataclass
class DataSource:
    structure_family: BaseStructureFamily
    structure: Any
    id: Optional[int] = None
    mimetype: Optional[str] = None
    parameters: dict = dataclasses.field(default_factory=dict)
    assets: List[Asset] = dataclasses.field(default_factory=list)
    management: Management = Management.writable
    name: Optional[str] = None

    @classmethod
    def from_json(cls, d):
        d = d.copy()
        assets = [Asset(**a) for a in d.pop("assets")]
        return cls(assets=assets, **d)


def validate_data_sources(node_structure_family, data_sources):
    "Check that data sources are consistent."
    return validators[node_structure_family](node_structure_family, data_sources)


def validate_container_data_sources(node_structure_family, data_sources):
    if len(data_sources) > 1:
        raise ValueError(
            "A container node can be backed by 0 or 1 data source, "
            f"not {len(data_sources)}"
        )
    return data_sources


def validate_consolidated_data_sources(node_structure_family, data_sources):
    "Check that column names and keys of others (e.g. arrays) do not collide."
    keys = set()
    names = set()
    for data_source in data_sources:
        if data_source.name is None:
            raise ValueError(
                "Data sources backing a consolidated structure_family must "
                "all have non-NULL names."
            )
        if data_source.name in names:
            raise ValueError(
                "Data sources must have unique names. "
                f"This name is used one more than one: {data_source.name}"
            )
        names.add(data_source.name)
        if data_source.structure_family == StructureFamily.table:
            columns = data_source.structure.columns
            if keys.intersection(columns):
                raise ValueError(
                    f"Data sources provide colliding keys: {keys.intersection(columns)}"
                )
            keys.update(columns)
        else:
            key = data_source.name
            if key in keys:
                raise ValueError(f"Data sources provide colliding keys: {key}")
            keys.add(key)
    return data_sources


def validate_other_data_sources(node_structure_family, data_sources):
    if len(data_sources) != 1:
        raise ValueError(
            f"A {node_structure_family} node must be backed by 1 data source."
        )
    return data_sources


validators = collections.defaultdict(lambda: validate_other_data_sources)
validators[StructureFamily.container] = validate_container_data_sources
validators[StructureFamily.consolidated] = validate_consolidated_data_sources
