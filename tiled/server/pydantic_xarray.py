from typing import Dict, List, Optional, Tuple, Union

from pydantic import BaseModel

from .pydantic_array import ArrayStructure


class DataArrayMacroStructure(BaseModel):
    variable: ArrayStructure
    coords: Optional[
        Dict[str, str]
    ]  # overridden below to be Optional[Dict[str, DataArrayStructure]]
    coord_names: List[str]
    name: str
    resizable: Union[bool, Tuple[bool, ...]] = False

    @classmethod
    def from_json(cls, structure):
        if structure["coords"] is not None:
            coords = {
                key: DataArrayStructure.from_json(value)
                for key, value in structure["coords"].items()
            }
        else:
            coords = None
        return cls(
            variable=ArrayStructure.from_json(structure["variable"]),
            coords=coords,
            coord_names=structure["coord_names"],
            name=structure["name"],
        )


class DataArrayStructure(BaseModel):
    macro: DataArrayMacroStructure
    micro: None

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=DataArrayMacroStructure.from_json(structure["macro"]), micro=None
        )


class DatasetMacroStructure(BaseModel):
    data_vars: Dict[str, DataArrayStructure]
    coords: Dict[str, DataArrayStructure]
    resizable: Union[bool, Tuple[bool, ...]] = False

    @classmethod
    def from_json(cls, structure):
        return cls(
            data_vars={
                key: DataArrayStructure.from_json(value)
                for key, value in structure["data_vars"].items()
            },
            coords={
                key: DataArrayStructure.from_json(value)
                for key, value in structure["coords"].items()
            },
        )


class DatasetStructure(BaseModel):
    macro: DatasetMacroStructure
    micro: None

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=DatasetMacroStructure.from_json(structure["macro"]), micro=None
        )
