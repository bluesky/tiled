from dataclasses import dataclass
import io
from typing import Dict, Tuple

import xarray

from .array import ArrayStructure
from .dataframe import (
    APACHE_ARROW_FILE_MIME_TYPE,
    serialize_arrow,
    serialize_csv,
    serialize_excel,
    serialize_html,
    XLSX_MIME_TYPE,
)
from ..media_type_registration import serialization_registry, deserialization_registry


@dataclass
class VariableMacroStructure:
    dims: Tuple[str]
    data: ArrayStructure
    attrs: Dict  # TODO Use JSONSerializableDict
    # TODO Variables also have `encoding`. Do we want to carry that as well?

    @classmethod
    def from_json(cls, structure):
        return cls(
            dims=structure["dims"],
            data=ArrayStructure.from_json(structure["data"]),
            attrs=structure["attrs"],
        )


@dataclass
class VariableStructure:
    macro: VariableMacroStructure
    micro: None

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=VariableMacroStructure.from_json(structure["macro"]), micro=None
        )


@dataclass
class DataArrayMacroStructure:
    variable: VariableStructure
    coords: Dict[str, VariableStructure]
    name: str

    @classmethod
    def from_json(cls, structure):
        return cls(
            variable=VariableStructure.from_json(structure["variable"]),
            coords={
                key: VariableStructure.from_json(value)
                for key, value in structure["coords"].items()
            },
            name=structure["name"],
        )


@dataclass
class DataArrayStructure:
    macro: DataArrayMacroStructure
    micro: None

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=DataArrayMacroStructure.from_json(structure["macro"]), micro=None
        )


@dataclass
class DatasetMacroStructure:
    data_vars: Dict[str, DataArrayStructure]
    coords: Dict[str, VariableStructure]
    attrs: Dict  # TODO Use JSONSerializableDict

    @classmethod
    def from_json(cls, structure):
        return cls(
            data_vars={
                key: DataArrayStructure.from_json(value)
                for key, value in structure["data_vars"].items()
            },
            coords={
                key: VariableStructure.from_json(value)
                for key, value in structure["coords"].items()
            },
            attrs=structure["attrs"],
        )


@dataclass
class DatasetStructure:
    macro: DatasetMacroStructure
    micro: None

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=DatasetMacroStructure.from_json(structure["macro"]), micro=None
        )


class _BytesIOThatIgnoresClose(io.BytesIO):
    def close(self):
        # When the netcdf writer tells us to close(), ignore it.
        pass


def serialize_netcdf(dataset):
    file = _BytesIOThatIgnoresClose()
    dataset.to_netcdf(file)  # TODO How would we expose options in the server?
    return file.getbuffer()


serialization_registry.register("dataset", "application/netcdf", serialize_netcdf)
# Support DataFrame formats by first converting to DataFrame.
# This doesn't make much sense for N-dimensional variables, but for
# 1-dimensional variables it is useful.
serialization_registry.register(
    "dataset",
    APACHE_ARROW_FILE_MIME_TYPE,
    lambda ds: serialize_arrow(ds.to_dataframe()),
)
serialization_registry.register(
    "dataset", "text/csv", lambda ds: serialize_csv(ds.to_dataframe())
)
serialization_registry.register(
    "dataset", XLSX_MIME_TYPE, lambda ds: serialize_excel(ds.to_dataframe())
)
serialization_registry.register(
    "dataset", "text/html", lambda ds: serialize_html(ds.to_dataframe())
)

deserialization_registry.register("dataset", "application/x-zarr", xarray.open_zarr)
# TODO How should we add support for access via Zarr?
