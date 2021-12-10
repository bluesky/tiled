import io
from dataclasses import dataclass
from typing import Dict

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
class DatasetMacroStructure:
    data_vars: Dict[str, ArrayStructure]
    coords: Dict[str, ArrayStructure]

    @classmethod
    def from_json(cls, structure):
        return cls(
            data_vars={
                key: ArrayStructure.from_json(value)
                for key, value in structure["data_vars"].items()
            },
            coords={
                key: ArrayStructure.from_json(value)
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


def serialize_netcdf(dataset, metadata):
    file = _BytesIOThatIgnoresClose()
    dataset.to_netcdf(file)  # TODO How would we expose options in the server?
    return file.getbuffer()


# Both application/netcdf and application/x-netcdf are used.
# https://en.wikipedia.org/wiki/NetCDF
serialization_registry.register("dataset", "application/netcdf", serialize_netcdf)
serialization_registry.register("dataset", "application/x-netcdf", serialize_netcdf)

# Support DataFrame formats by first converting to DataFrame.
# This doesn't make much sense for N-dimensional variables, but for
# 1-dimensional variables it is useful.
serialization_registry.register(
    "dataset",
    APACHE_ARROW_FILE_MIME_TYPE,
    lambda ds, metadata: serialize_arrow(ds.to_dataframe(), metadata),
)
serialization_registry.register(
    "dataset",
    "application/x-parquet",
    lambda ds, metadata: serialize_parquet(ds.to_dataframe(), metadata),
)
serialization_registry.register(
    "dataset",
    "text/csv",
    lambda ds, metadata: serialize_csv(ds.to_dataframe(), metadata),
)
serialization_registry.register(
    "dataset",
    "text/plain",
    lambda ds, metadata: serialize_csv(ds.to_dataframe(), metadata),
)
serialization_registry.register(
    "dataset",
    "text/html",
    lambda ds, metadata: serialize_html(ds.to_dataframe(), metadata),
)
serialization_registry.register(
    "dataset",
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
        "dataset",
        "application/json",
        serialize_json,
    )

deserialization_registry.register(
    "dataset", "application/x-zarr", lambda ds, metadata: xarray.open_zarr(ds)
)
# TODO How should we add support for access via Zarr?
