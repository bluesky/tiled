from dataclasses import dataclass
import enum
import sys
from typing import Tuple

import dask.array
import numpy


class Endianness(str, enum.Enum):
    big = "big"
    little = "little"
    not_applicable = "not_applicable"


class Kind(str, enum.Enum):
    """
    See https://numpy.org/devdocs/reference/arrays.interface.html#object.__array_interface__

    The term "kind" comes from the numpy API as well.
    """

    bit_field = "t"
    boolean = "b"
    integer = "i"
    unsigned_integer = "ui"
    floating_point = "f"
    complex_floating_point = "c"
    timedelta = "m"
    datetime = "M"
    string = "S"  # fixed-length sequence of char
    unicode = "U"  # fixed-length sequence of Py_UNICODE
    other = "V"  # "V" is for "void" -- generic fixed-size chunk of memory


@dataclass
class MachineDataType:
    endianness: Endianness
    kind: Kind
    itemsize: int

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
            kind=dtype.kind,
            itemsize=dtype.itemsize,
        )

    def to_numpy_dtype(self):
        endianness = self.__endianness_reverse_map[self.endianness]
        return numpy.dtype(f"{endianness}{self.kind.value}{self.itemsize}")


@dataclass
class ArrayStructure:
    dtype: MachineDataType
    chunks: Tuple[Tuple[int, ...], ...]  # tuple-of-tuples-of-ints like ((3,), (3,))
    shape: Tuple[int, ...]  # tuple-of-ints like (3, 3)


class ArraySource:

    container = "array"

    def __init__(self, data):
        self.metadata = {}
        if not isinstance(data, dask.array.Array):
            data = dask.array.from_array(data)
        self._data = data

    def __repr__(self):
        return f"{type(self).__name__}({self._data!r})"

    def describe(self):
        return ArrayStructure(
            shape=self._data.shape,
            chunks=self._data.chunks,
            dtype=MachineDataType.from_numpy_dtype(self._data.dtype),
        )

    def read(self):
        return self._data
