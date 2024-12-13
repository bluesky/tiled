from typing import Any, List, Optional

import pydantic

from ..structures.core import StructureFamily


class ConsolidatedStructurePart(pydantic.BaseModel):
    structure_family: StructureFamily
    structure: Any  # Union of Structures, but we do not want to import them...
    name: str

    @classmethod
    def from_json(cls, item):
        return cls(**item)


class ConsolidatedStructure(pydantic.BaseModel):
    parts: List[ConsolidatedStructurePart]
    all_keys: Optional[List[str]]

    @classmethod
    def from_json(cls, structure):
        return cls(
            parts=[
                ConsolidatedStructurePart.from_json(item) for item in structure["parts"]
            ],
            all_keys=structure["all_keys"],
        )

    @classmethod
    def from_data_sources(cls, data_sources):
        all_keys = []
        for data_source in data_sources:
            if data_source.structure_family == StructureFamily.table:
                all_keys.extend(data_source.structure.columns)
            else:
                all_keys.append(data_source.name)
        parts = [
            ConsolidatedStructurePart(
                data_source_id=data_source.id,
                structure=data_source.structure,
                structure_family=data_source.structure_family,
                name=data_source.name,
            )
            for data_source in data_sources
        ]

        return cls(parts=parts, all_keys=all_keys)
