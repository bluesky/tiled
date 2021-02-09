import enum
import sys

import numpy
import pydantic
import pydantic.generics
from typing import Generic, Optional, Type, TypeVar, Tuple, Any


DataT = TypeVar("DataT")


class Error(pydantic.BaseModel):
    code: int
    message: str


class Response(pydantic.generics.GenericModel, Generic[DataT]):
    data: Optional[DataT]
    error: Optional[Error]
    meta: Optional[dict]
    links: Optional[dict]

    @pydantic.validator("error", always=True)
    def check_consistency(cls, v, values):
        if v is not None and values["data"] is not None:
            raise ValueError("must not provide both data and error")
        if v is None and values.get("data") is None:
            raise ValueError("must provide data or error")
        return v


class EntryType(str, enum.Enum):
    catalog = "catalog"
    datasource = "datasource"


class EntryFields(str, enum.Enum):
    metadata = "metadata"
    structure = "structure"
    count = "count"
    none = ""


class CatalogAttributes(pydantic.BaseModel):
    metadata: Optional[dict]  # free-form, user-specified dict
    count: Optional[int]


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


class MachineDataType(pydantic.BaseModel):
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


class DataSourceStructure(pydantic.BaseModel):
    dtype: MachineDataType
    chunks: Tuple[Tuple[int, ...], ...]  # tuple-of-tuples-of-ints like ((3,), (3,))
    shape: Tuple[int, ...]  # tuple-of-ints like (3, 3)


class DataSourceAttributes(pydantic.BaseModel):
    metadata: Optional[dict]  # free-form, user-specified dict
    structure: Optional[DataSourceStructure]


class Resource(pydantic.BaseModel):
    "A JSON API Resource"
    id: str
    type: EntryType
    meta: dict


class CatalogResource(Resource):
    "Representation of a Catalog as a JSON API Resource"
    attributes: CatalogAttributes


class DataSourceResource(Resource):
    "Representation of a DataSource as a JSON API Resource"
    attributes: DataSourceAttributes


BlockIndex = Tuple
