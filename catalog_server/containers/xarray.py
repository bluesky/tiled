from dataclasses import dataclass
from typing import Dict, Tuple

from .array import ArrayStructure


@dataclass
class VariableStructure:
    dims: Tuple[str]
    data: ArrayStructure
    attrs: Dict  # TODO Use JSONSerializableDict
    # TODO Variables also have `encoding`. Do we want to carry that as well?


@dataclass
class DataArrayStructure:
    variable: VariableStructure
    coords: Dict[str, VariableStructure]
    name: str


@dataclass
class DatasetStructure:
    data_vars: Dict[str, DataArrayStructure]
    coords: Dict[str, VariableStructure]
    attrs: Dict  # TODO Use JSONSerializableDict


# TODO Also support zarr for encoding.
