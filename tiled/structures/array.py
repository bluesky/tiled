import enum
import os
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

import numpy


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
class BuiltinDtype:
    endianness: Endianness
    kind: Kind
    itemsize: int
    dt_units: Optional[str] = None

    __endianness_map = {
        ">": "big",
        "<": "little",
        "=": sys.byteorder,
        "|": "not_applicable",
    }

    __endianness_reverse_map = {"big": ">", "little": "<", "not_applicable": "|"}

    @classmethod
    def from_numpy_dtype(cls, dtype) -> "BuiltinDtype":
        # Extract datetime units from the dtype string representation,
        # e.g. `'<M8[ns]'` has `dt_units = '[ns]'`. Count determines the number of base units in a step.
        dt_units = None
        if dtype.kind in ("m", "M"):
            unit, count = numpy.datetime_data(dtype)
            dt_units = f"[{count if count > 1 else ''}{unit}]"

        return cls(
            endianness=cls.__endianness_map[dtype.byteorder],
            kind=Kind(dtype.kind),
            itemsize=dtype.itemsize,
            dt_units=dt_units,
        )

    def to_numpy_dtype(self) -> numpy.dtype:
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
        return f"{endianness}{self.kind.value}{size}{self.dt_units or ''}"

    def to_numpy_descr(self):
        "An alias for to_numpy_str() to match the StructDtype interface."
        return self.to_numpy_str()

    @classmethod
    def from_json(cls, structure):
        return cls(
            kind=Kind(structure["kind"]),
            itemsize=structure["itemsize"],
            endianness=Endianness(structure["endianness"]),
            dt_units=structure.get("dt_units"),
        )


@dataclass
class Field:
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

    @classmethod
    def from_array(cls, array, shape=None, chunks=None, dims=None) -> "ArrayStructure":
        from dask.array.core import normalize_chunks

        if not hasattr(array, "__array__"):
            # may be a list of something; convert to array
            array = numpy.asanyarray(array)

        # Why would shape ever be different from array.shape, you ask?
        # Some formats (notably Zarr) force shape to be a multiple of
        # a chunk size, such that array.shape may include a margin beyond the
        # actual data.
        if shape is None:
            shape = array.shape
        if chunks is None:
            if hasattr(array, "chunks"):
                chunks = array.chunks  # might be None
            else:
                chunks = None
            if chunks is None:
                chunks = ("auto",) * len(shape)
        normalized_chunks = normalize_chunks(
            chunks,
            shape=shape,
            dtype=array.dtype,
        )
        if array.dtype.fields is not None:
            data_type = StructDtype.from_numpy_dtype(array.dtype)
        else:
            data_type = BuiltinDtype.from_numpy_dtype(array.dtype)
        return ArrayStructure(
            data_type=data_type,
            shape=shape,
            chunks=normalized_chunks,
            dims=dims,
        )
