import dataclasses
from typing import Optional


@dataclasses.dataclass
class ContainerStructure:
    contents: Optional[dict]
    count: Optional[int]

    @classmethod
    def from_json(cls, structure):
        return cls(**structure)
