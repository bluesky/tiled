from dataclasses import dataclass
from typing import Dict, Tuple

from .array import ArrayStructure


@dataclass
class VariableStructure:
    dims: Tuple[str]
    data: ArrayStructure
    attrs: dict
    # Variables also have `encoding`. Do we want to carry that as well?


@dataclass
class DataArrayStructure:
    dims: Tuple[str]
    data: ArrayStructure
    coords: Dict[str, VariableStructure]
    attrs: Dict
    name: str


@dataclass
class DatasetStructure:
    dims: Tuple[str]
    data_vars: DataArrayStructure
    coords: Dict[str, DataArrayStructure]
    attrs: dict


# TODO Also support zarr for encoding.
