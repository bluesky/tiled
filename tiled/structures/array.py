import base64
from dataclasses import dataclass
import enum
import io
import json
import sys
from typing import Tuple

import numpy

from ..media_type_registration import serialization_registry, deserialization_registry
from ..utils import modules_available


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
class ArrayMacroStructure:
    chunks: Tuple[Tuple[int, ...], ...]  # tuple-of-tuples-of-ints like ((3,), (3,))
    shape: Tuple[int, ...]  # tuple-of-ints like (3, 3)

    @classmethod
    def from_json(cls, structure):
        return cls(
            chunks=tuple(map(tuple, structure["chunks"])),
            shape=tuple(structure["shape"]),
        )


@dataclass
class ArrayStructure:
    macro: ArrayMacroStructure
    micro: MachineDataType

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=ArrayMacroStructure.from_json(structure["macro"]),
            micro=MachineDataType.from_json(structure["micro"]),
        )


serialization_registry.register("array", "application/octet-stream", memoryview)
serialization_registry.register(
    "array", "application/json", lambda array: json.dumps(array.tolist()).encode()
)
deserialization_registry.register(
    "array",
    "application/octet-stream",
    lambda buffer, dtype, shape: numpy.frombuffer(buffer, dtype=dtype).reshape(shape),
)
if modules_available("PIL"):

    def save_to_buffer_PIL(array, format):
        from PIL import Image

        # Handle too *few* dimensions here, and let PIL raise if there are too
        # *many* because it depends on the shape (RGB, RGBA, etc.)
        normalized_array = numpy.atleast_2d(array)
        file = io.BytesIO()
        image = Image.fromarray(normalized_array).convert("RGBA")
        image.save(file, format=format)
        return file.getbuffer()

    def array_from_buffer_PIL(buffer, format, dtype, shape):
        from PIL import Image

        file = io.BytesIO(buffer)
        image = Image.open(file, format=format)
        return numpy.asarray(image).asdtype(dtype).reshape(shape)

    serialization_registry.register(
        "array",
        "image/png",
        lambda array: save_to_buffer_PIL(array, "png"),
    )
    deserialization_registry.register(
        "array",
        "image/png",
        lambda buffer, dtype, shape: array_from_buffer_PIL(buffer, "png", dtype, shape),
    )
    serialization_registry.register(
        "array",
        "text/html",
        lambda array: (
            "<html>"
            '<img src="data:image/png;base64,'
            f"{base64.b64encode(save_to_buffer_PIL(array, 'png')).decode()!s}\""
            "/>"
            "</html>"
        ),
    )
if modules_available("tifffile"):

    def array_from_buffer_tifffile(buffer, dtype, shape):
        from tifffile import imread

        return imread(buffer).astype(dtype).reshape(shape)

    def save_to_buffer_tifffile(array):
        from tifffile import imsave

        # Handle too *few* dimensions here, and let tifffile raise if there are too
        # *many* because it depends on the shape (RGB, RGBA, etc.)
        normalized_array = numpy.atleast_2d(array)
        file = io.BytesIO()
        imsave(file, normalized_array)
        return file.getbuffer()

    serialization_registry.register(
        "array",
        "image/tiff",
        save_to_buffer_tifffile,
    )
    deserialization_registry.register(
        "array",
        "image/tiff",
        array_from_buffer_tifffile,
    )
