"""
This module was created to save structure methods that can be handled by
the server and the client.

"""

import enum
from dataclasses import asdict, dataclass
from typing import Optional


class StructureFamily(str, enum.Enum):
    awkward = "awkward"
    container = "container"
    array = "array"
    sparse = "sparse"
    table = "table"


@dataclass(frozen=True)
class Spec:
    name: str
    version: Optional[str] = None

    def __init__(self, name, version=None):
        # Enable the name to be passed as a position argument.
        # The setattr stuff is necessary to make this work with a frozen dataclass.
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "version", version)

    def __repr__(self):
        # Display the name as a positional argument, for conciseness.
        if self.version is None:
            output = f"{type(self).__name__}({self.name!r})"
        else:
            output = f"{type(self).__name__}({self.name!r}, version={self.version!r})"
        return output

    def dict(self):
        return asdict(self)
