import enum
import os
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union


class Endianness(str, enum.Enum):
    """
    An enum of endian values: big, little, not_applicable.
    """

    big = "big"
    little = "little"
    not_applicable = "not_applicable"


class ObjectArrayTypeDisabled(ValueError):
    pass


class Kind(str, enum.Enum):
    """
    See https://numpy.org/devdocs/reference/arrays.interface.html#object.__array_interface__

    The term "kind" comes from the numpy API as well.

    Note: At import time, the environment variable ``TILED_ALLOW_OBJECT_ARRAYS``
    is checked. If it is set to anything other than ``"0"``, then this
    Enum gets an additional member::

        object = "O"

    to support numpy 'object'-type arrays which hold generic Python objects.
    Numpy 'object'-type arrays are not enabled by default because their binary
    representation is not interpretable by clients other than Python.  It is
    recommended to convert your data to a non-object type if possible so that it
    can be read by non-Python clients.
    """

    bit_field = "t"
    boolean = "b"
    integer = "i"
    unsigned_integer = "u"
    floating_point = "f"
    complex_floating_point = "c"
    timedelta = "m"
    datetime = "M"
    string = "S"  # fixed-length sequence of char
    unicode = "U"  # fixed-length sequence of Py_UNICODE
    other = "V"  # "V" is for "void" -- generic fixed-size chunk of memory

    # By default, do not tolerate numpy objectg arrays
    if os.getenv("TILED_ALLOW_OBJECT_ARRAYS", "0") != "0":
        object = "O"  # Object (i.e. the memory contains a pointer to PyObject)

    @classmethod
    def _missing_(cls, key):
        if key == "O":
            raise ObjectArrayTypeDisabled(
                "Numpy 'object'-type arrays are not enabled by default "
                "because their binary representation is not interpretable "
                "by clients other than Python. "
                "It is recommended to convert your data to a non-object type "
                "if possible so that it can be read by non-Python clients. "
                "If this is not possible, you may enable 'object'-type arrays "
                "by setting the environment variable TILED_ALLOW_OBJECT_ARRAYS=1 "
                "in the server."
            )


@dataclass
class ArrayMacroStructure:
    chunks: Tuple[Tuple[int, ...], ...]  # tuple-of-tuples-of-ints like ((3,), (3,))
    shape: Tuple[int, ...]  # tuple of ints like (3, 3)
    dims: Optional[Tuple[str, ...]] = None  # None or tuple of names like ("x", "y")
    resizable: Union[bool, Tuple[bool, ...]] = False

    @classmethod
    def from_json(cls, structure):
        dims = structure["dims"]
        if dims is not None:
            dims = tuple(dims)
        return cls(
            chunks=tuple(map(tuple, structure["chunks"])),
            shape=tuple(structure["shape"]),
            dims=dims,
            resizable=structure.get("resizable", False),
        )


@dataclass
class BuiltinDtype:
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
        import numpy

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


@dataclass
class Field:
    name: str
    dtype: Union[BuiltinDtype, "StructDtype"]
    shape: Optional[Tuple[int, ...]]

    @classmethod
    def from_numpy_descr(cls, field):
        import numpy

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
        # If this is a builtin type, require the use of BuiltinDtype (nee .array.BuiltinDtype)
        if dtype.fields is None:
            raise ValueError(f"You have a base type: {dtype}")
        return cls(
            itemsize=dtype.itemsize,
            fields=[Field.from_numpy_descr(f) for f in dtype.descr],
        )

    def to_numpy_dtype(self):
        import numpy

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


@dataclass
class ArrayStructure:
    macro: ArrayMacroStructure
    micro: Union[BuiltinDtype, StructDtype]

    @classmethod
    def from_json(cls, structure):
        if "fields" in structure["micro"]:
            micro = StructDtype.from_json(structure["micro"])
        else:
            micro = BuiltinDtype.from_json(structure["micro"])
        return cls(
            macro=ArrayMacroStructure.from_json(structure["macro"]),
            micro=micro,
        )
