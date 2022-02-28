import io
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import xarray

from ..media_type_registration import deserialization_registry, serialization_registry
from ..utils import modules_available
from .array import ArrayStructure
from .dataframe import (
    APACHE_ARROW_FILE_MIME_TYPE,
    XLSX_MIME_TYPE,
    serialize_arrow,
    serialize_csv,
    serialize_excel,
    serialize_html,
    serialize_parquet,
)


@dataclass
class DataArrayMacroStructure:
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


@dataclass
class DataArrayStructure:
    macro: DataArrayMacroStructure
    micro: None

    @classmethod
    def from_json(cls, structure):
        return cls(
            macro=DataArrayMacroStructure.from_json(structure["macro"]), micro=None
        )


# Define a nested structure now that the necessary object has been defined.
DataArrayMacroStructure.__annotations__[
    "coords"
] = DataArrayMacroStructure.__annotations__["coords"].copy_with(
    (str, DataArrayMacroStructure)
)


@dataclass
class DatasetMacroStructure:
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


if modules_available("h5netcdf"):

    def serialize_netcdf(dataset, metadata):
        file = _BytesIOThatIgnoresClose()
        # This engine is reportedly faster.
        # Also, by avoiding the default engine, we avoid a dependency on 'scipy'.
        dataset.to_netcdf(file, engine="h5netcdf")
        return file.getbuffer()

    # Both application/netcdf and application/x-netcdf are used.
    # https://en.wikipedia.org/wiki/NetCDF
    serialization_registry.register(
        "xarray_dataset", "application/netcdf", serialize_netcdf
    )
    serialization_registry.register(
        "xarray_dataset", "application/x-netcdf", serialize_netcdf
    )

# Support DataFrame formats by first converting to DataFrame.
# This doesn't make much sense for N-dimensional variables, but for
# 1-dimensional variables it is useful.
serialization_registry.register(
    "xarray_dataset",
    APACHE_ARROW_FILE_MIME_TYPE,
    lambda ds, metadata: serialize_arrow(ds.to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "application/x-parquet",
    lambda ds, metadata: serialize_parquet(ds.to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "text/csv",
    lambda ds, metadata: serialize_csv(ds.to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "text/plain",
    lambda ds, metadata: serialize_csv(ds.to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "text/html",
    lambda ds, metadata: serialize_html(ds.to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    XLSX_MIME_TYPE,
    lambda ds, metadata: serialize_excel(ds.to_dataframe(), metadata),
)
if modules_available("orjson"):
    import orjson

    def serialize_json(ds, metadata):
        df = ds.to_dataframe()
        return orjson.dumps(
            {column: df[column].tolist() for column in df},
        )

    serialization_registry.register(
        "xarray_dataset",
        "application/json",
        serialize_json,
    )

deserialization_registry.register(
    "xarray_dataset", "application/x-zarr", lambda ds, metadata: xarray.open_zarr(ds)
)
# TODO How should we add support for access via Zarr?
