"""
This module was created to save structure methods that can be handled by
the server and the client.

"""

import enum
import importlib
from dataclasses import asdict, dataclass
from typing import Dict, Optional

from pydantic import StringConstraints
from typing_extensions import Annotated

from ..utils import OneShotCachedMap


class StructureFamily(str, enum.Enum):
    array = "array"
    awkward = "awkward"
    container = "container"
    sparse = "sparse"
    table = "table"


@dataclass(frozen=True)
class Spec:
    name: Annotated[str, StringConstraints(max_length=255)]
    version: Optional[Annotated[str, StringConstraints(max_length=255)]] = None

    def __init__(self, name, version=None) -> None:
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

    def dict(self) -> Dict[str, Optional[str]]:
        # For easy interoperability with pydantic 1.x models
        return asdict(self)

    model_dump = dict  # For easy interoperability with pydantic 2.x models


# TODO: make type[Structure] after #1036
STRUCTURE_TYPES = OneShotCachedMap[StructureFamily, type](
    {
        StructureFamily.array: lambda: importlib.import_module(
            "...structures.array", StructureFamily.__module__
        ).ArrayStructure,
        StructureFamily.awkward: lambda: importlib.import_module(
            "...structures.awkward", StructureFamily.__module__
        ).AwkwardStructure,
        StructureFamily.table: lambda: importlib.import_module(
            "...structures.table", StructureFamily.__module__
        ).TableStructure,
        StructureFamily.sparse: lambda: importlib.import_module(
            "...structures.sparse", StructureFamily.__module__
        ).SparseStructure,
    }
)
