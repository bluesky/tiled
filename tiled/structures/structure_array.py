from dataclasses import dataclass
import sys
from typing import Tuple, List, Union, Optional

import numpy

from .array import Endianness, Kind, MachineDataType as BuiltinType, ArrayMacroStructure
from .dataframe import DataFrameMacroStructure


from ..media_type_registration import serialization_registry, deserialization_registry


@dataclass
class Field:
    name: str
    dtype: Union[BuiltinType, "MachineDataType"]
    subshape: Optional[Tuple[int, ...]]


@dataclass
class MachineDataType:
    endianness: Endianness
    kind: Kind
    itemsize: int
    descr: List[Field]

    __endianness_map = {
        ">": "big",
        "<": "little",
        "=": sys.byteorder,
        "|": "not_applicable",
    }

    __endianness_reverse_map = {
        "big": ">",
        "little": "<",
        "not_applicable": "|",
    }

    @classmethod
    def from_numpy_dtype(cls, dtype):
        return cls(
            endianness=cls.__endianness_map[dtype.byteorder],
            kind=Kind(dtype.kind),
            itemsize=dtype.itemsize,
        )

    def to_numpy_dtype(self):
        endianness = self.__endianness_reverse_map[self.endianness]
        return numpy.dtype(f"{endianness}{self.kind.value}{self.itemsize}")

    @classmethod
    def from_json(cls, structure):
        return cls(
            kind=Kind(structure["kind"]),
            itemsize=structure["itemsize"],
            endianness=Endianness(structure["endianness"]),
        )


@dataclass
class StructureArrayStructure:
    macro: ArrayMacroStructure
    micro: MachineDataType

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=ArrayMacroStructure.from_json(structure["macro"]),
            micro=MachineDataType.from_json(structure["micro"]),
        )


@dataclass
class StructureArrayStructure1D:
    macro: DataFrameMacroStructure
    micro: MachineDataType

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=ArrayMacroStructure.from_json(structure["macro"]),
            micro=MachineDataType.from_json(structure["micro"]),
        )
