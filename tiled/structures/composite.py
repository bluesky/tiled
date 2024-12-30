import dataclasses
from typing import Any, List, Optional

from .core import StructureFamily

@dataclasses.dataclass
class CompositeStructure:
    contents: Optional[dict]
    count: Optional[int]
    flat_keys: List[str] = dataclasses.field(default_factory=list)

    @classmethod
    def from_json(cls, structure):
        return cls(**structure)

    def __post_init__(self):
        if self.contents is not None:
            for key, item in self.contents.items():
                if item['attributes']['structure_family'] in [StructureFamily.array, StructureFamily.awkward, StructureFamily.sparse]:
                    self.flat_keys.append(key)
                elif item['attributes']['structure_family'] == StructureFamily.table:
                    self.flat_keys.extend(item['attributes']['structure']['columns'])
        unique_keys = set(self.flat_keys)
        self.flat_keys.clear()
        self.flat_keys.extend(unique_keys)
