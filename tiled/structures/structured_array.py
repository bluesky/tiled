from dataclasses import dataclass
import json
from typing import Tuple, List, Union, Optional

import numpy

from .array import MachineDataType as BuiltinType, ArrayMacroStructure
from ..media_type_registration import serialization_registry


@dataclass
class Field:
    name: str
    dtype: Union[BuiltinType, "StructDtype"]
    shape: Optional[Tuple[int, ...]]

    @classmethod
    def from_numpy_descr(cls, field):
        name, *rest = field
        if name == "":
            raise ValueError(
                f"You seem to have gotten descr of a base or subdtype: {field}"
            )
        if len(rest) == 1:
            (f_type,) = rest
            shape = None
        else:
            f_type, shape = rest

        if isinstance(f_type, str):
            FType = BuiltinType.from_numpy_dtype(numpy.dtype(f_type))
        else:
            FType = StructDtype.from_numpy_dtype(numpy.dtype(f_type))
        return cls(name=name, dtype=FType, shape=shape)

    def to_numpy_descr(self):
        if isinstance(self.dtype, BuiltinType):
            base = [self.name, self.dtype.to_numpy_str()]
        else:
            base = [self.name, self.dtype.to_numpy_descr()]
        if self.shape is None:
            return tuple(base)
        else:
            return tuple(base + [self.shape])

    @classmethod
    def from_json(cls, structure):
        name = structure["name"]
        if "fields" in structure["dtype"]:
            ftype = StructDtype.from_json(structure["dtype"])
        else:
            ftype = BuiltinType.from_json(structure["dtype"])
        return cls(name=name, dtype=ftype, shape=structure["shape"])


@dataclass
class StructDtype:
    itemsize: int
    fields: List[Field]

    @classmethod
    def from_numpy_dtype(cls, dtype):
        # subdtypes push extra dimensions into arrays, we should handle these
        # a layer up and report an array with bigger dimensions.
        if dtype.subdtype is not None:
            raise ValueError(f"We do not know how to encode subdtypes: {dtype}")
        # If this is a builtin type, require the use of BuiltinType (nee .array.MachineDataType)
        if dtype.fields is None:
            raise ValueError(f"You have a base type: {dtype}")
        return cls(
            itemsize=dtype.itemsize,
            fields=[Field.from_numpy_descr(f) for f in dtype.descr],
        )

    def to_numpy_dtype(self):
        return numpy.dtype(self.to_numpy_descr())

    def to_numpy_descr(self):
        return [f.to_numpy_descr() for f in self.fields]

    def max_depth(self):
        return max(
            1 if isinstance(f.dtype, BuiltinType) else 1 + f.dtype.max_depth()
            for f in self.fields
        )

    @classmethod
    def from_json(cls, structure):
        return cls(
            itemsize=structure["itemsize"],
            fields=[Field.from_json(f) for f in structure["fields"]],
        )


@dataclass
class StructuredArrayGenericStructure:
    macro: ArrayMacroStructure
    micro: StructDtype

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=ArrayMacroStructure.from_json(structure["macro"]),
            micro=StructDtype.from_json(structure["micro"]),
        )


@dataclass
class ArrayTabularMacroStructure:
    """
    Similar to ArrayMacroStructure, but must be 1D

    This is distinct from DataFrameMacoStructure because it knows its length and
    chunk sizes. Dataframes only know number of partitions.
    """

    chunks: Tuple[Tuple[int]]
    shape: Tuple[int]

    @classmethod
    def from_json(cls, structure):
        return cls(
            chunks=tuple(map(tuple, structure["chunks"])),
            shape=tuple(structure["shape"]),
        )


@dataclass
class StructuredArrayTabularStructure:
    macro: ArrayTabularMacroStructure
    micro: StructDtype

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=ArrayMacroStructure.from_json(structure["macro"]),
            micro=StructDtype.from_json(structure["micro"]),
        )


serialization_registry.register(
    "structured_array_generic",
    "application/octet-stream",
    lambda array, metadata: memoryview(numpy.ascontiguousarray(array)),
)
serialization_registry.register(
    "structured_array_generic",
    "application/json",
    lambda array, metadata: json.dumps(array.tolist()).encode(),
)
serialization_registry.register(
    "structured_array_tabular",
    "application/octet-stream",
    lambda array, metadata: memoryview(numpy.ascontiguousarray(array)),
)
serialization_registry.register(
    "structured_array_tabular",
    "application/json",
    lambda array, metadata: json.dumps(array.tolist()).encode(),
)
