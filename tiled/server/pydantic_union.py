from typing import Any, List, Optional

import pydantic

from ..structures.core import StructureFamily


class UnionStructurePart(pydantic.BaseModel):
    structure_family: StructureFamily
    structure: Any  # Union of Structures, but we do not want to import them...
    name: str

    @classmethod
    def from_json(cls, item):
        return cls(**item)


class UnionStructure(pydantic.BaseModel):
    parts: List[UnionStructurePart]
    all_keys: Optional[List[str]]

    @classmethod
    def from_json(cls, structure):
        return cls(
            parts=[UnionStructurePart.from_json(item) for item in structure["parts"]],
            all_keys=structure["all_keys"],
        )
