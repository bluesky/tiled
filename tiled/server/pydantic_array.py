"""
The ArrayStructure class in tiled.structure.array is implemeneted with Python
built-in dataclasses. This is an implementation of the same structure in pydantic.

FastAPI requires pydantic models specifically, and will attempt to automatically
convert dataclasses to pydantic models. This normally works fine, but in the
case of ArrayStructure specifically it fails because of this issue:

    https://github.com/samuelcolvin/pydantic/issues/3251

Once this problem is fixed in pydantic, this separate implementation in
ArrayStructure will no longer by needed. We will turn this module into a shim
that just imports ArrayStructure from tiled.structures.array and then deprecate
it.
"""

import sys
from typing import List, Optional, Tuple, Union

import numpy
from pydantic import BaseModel

from ..structures.array import Endianness, Kind


class BuiltinDtype(BaseModel):
    endianness: Endianness
    kind: Kind
    itemsize: int

    __endianness_map = {
        ">": "big",
        "<": "little",
        "=": sys.byteorder,
        "|": "not_applicable",
    }

    __endianness_reverse_map = {"big": ">", "little": "<", "not_applicable": "|"}

    @classmethod
    def from_numpy_dtype(cls, dtype):
        return cls(
            endianness=cls.__endianness_map[dtype.byteorder],
            kind=Kind(dtype.kind),
            itemsize=dtype.itemsize,
        )

    def to_numpy_dtype(self):
        return numpy.dtype(self.to_numpy_str())

    def to_numpy_str(self):
        endianness = self.__endianness_reverse_map[self.endianness]
        # dtype.itemsize always reports bytes.  The format string from the
        # numeric types the string format is: {type_code}{byte_count} so we can
        # directly use the item size.
        #
        # for unicode the pattern is 'U{char_count}', however
        # which numpy always represents as 4 byte UCS4 encoding
        # (because variable width encodings do not fit with fixed-stride arrays)
        # so the reported itemsize is 4x the char count.  To get back to the string
        # we need to divide by 4.
        size = self.itemsize if self.kind != Kind.unicode else self.itemsize // 4
        return f"{endianness}{self.kind.value}{size}"

    @classmethod
    def from_json(cls, structure):
        return cls(
            kind=Kind(structure["kind"]),
            itemsize=structure["itemsize"],
            endianness=Endianness(structure["endianness"]),
        )


class Field(BaseModel):
    name: str
    dtype: Union[BuiltinDtype, "StructDtype"]
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
            FType = BuiltinDtype.from_numpy_dtype(numpy.dtype(f_type))
        else:
            FType = StructDtype.from_numpy_dtype(numpy.dtype(f_type))
        return cls(name=name, dtype=FType, shape=shape)

    def to_numpy_descr(self):
        if isinstance(self.dtype, BuiltinDtype):
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
            ftype = BuiltinDtype.from_json(structure["dtype"])
        return cls(name=name, dtype=ftype, shape=structure["shape"])


class StructDtype(BaseModel):
    itemsize: int
    fields: List[Field]

    @classmethod
    def from_numpy_dtype(cls, dtype):
        # subdtypes push extra dimensions into arrays, we should handle these
        # a layer up and report an array with bigger dimensions.
        if dtype.subdtype is not None:
            raise ValueError(f"We do not know how to encode subdtypes: {dtype}")
        # If this is a builtin type, require the use of BuiltinDtype (nee .array.BuiltinDtype)
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
            1 if isinstance(f.dtype, BuiltinDtype) else 1 + f.dtype.max_depth()
            for f in self.fields
        )

    @classmethod
    def from_json(cls, structure):
        return cls(
            itemsize=structure["itemsize"],
            fields=[Field.from_json(f) for f in structure["fields"]],
        )


Field.update_forward_refs()


class ArrayStructure(BaseModel):
    data_type: Union[BuiltinDtype, StructDtype]
    chunks: Tuple[Tuple[int, ...], ...]  # tuple-of-tuples-of-ints like ((3,), (3,))
    shape: Tuple[int, ...]  # tuple of ints like (3, 3)
    dims: Optional[Tuple[str, ...]] = None  # None or tuple of names like ("x", "y")
    resizable: Union[bool, Tuple[bool, ...]] = False

    @classmethod
    def from_json(cls, structure):
        if "fields" in structure["data_type"]:
            data_type = StructDtype.from_json(structure["data_type"])
        else:
            data_type = BuiltinDtype.from_json(structure["data_type"])
        dims = structure["dims"]
        if dims is not None:
            dims = tuple(dims)
        return cls(
            data_type=data_type,
            chunks=tuple(map(tuple, structure["chunks"])),
            shape=tuple(structure["shape"]),
            dims=dims,
            resizable=structure.get("resizable", False),
        )
