import dataclasses
from typing import Any, List, Optional

from .core import StructureFamily


@dataclasses.dataclass
class UnionStructurePart:
    structure_family: StructureFamily
    structure: Any  # Union of Structures, but we do not want to import them...
    name: Optional[str]

    @classmethod
    def from_json(cls, item):
        return cls(**item)


@dataclasses.dataclass
class UnionStructure:
    parts: List[UnionStructurePart]
    all_keys: List[str]

    @classmethod
    def from_json(cls, structure):
        return cls(
            parts=[UnionStructurePart.from_json(item) for item in structure["parts"]],
            all_keys=structure["all_keys"],
        )
