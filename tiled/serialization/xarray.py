import io

from ..media_type_registration import serialization_registry
from ..utils import modules_available
from .dataframe import (
    APACHE_ARROW_FILE_MIME_TYPE,
    XLSX_MIME_TYPE,
    serialize_arrow,
    serialize_csv,
    serialize_excel,
    serialize_html,
    serialize_parquet,
)


class _BytesIOThatIgnoresClose(io.BytesIO):
    def close(self):
        # When the netcdf writer tells us to close(), ignore it.
        pass


if modules_available("scipy"):

    def serialize_netcdf(node, metadata):
        file = _BytesIOThatIgnoresClose()
        # This engine is reportedly faster.
        # Also, by avoiding the default engine, we avoid a dependency on 'scipy'.
        node.as_dataset().to_netcdf(file, engine="scipy")
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
    lambda node, metadata: serialize_arrow(node.as_dataset().to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "application/x-parquet",
    lambda node, metadata: serialize_parquet(
        node.as_dataset().to_dataframe(), metadata
    ),
)
serialization_registry.register(
    "xarray_dataset",
    "text/csv",
    lambda node, metadata: serialize_csv(node.as_dataset().to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "text/plain",
    lambda node, metadata: serialize_csv(node.as_dataset().to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "text/html",
    lambda node, metadata: serialize_html(node.as_dataset().to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    XLSX_MIME_TYPE,
    lambda node, metadata: serialize_excel(node.as_dataset().to_dataframe(), metadata),
)
if modules_available("orjson"):
    import orjson

    def serialize_json(node, metadata):
        df = node.as_dataset().to_dataframe()
        return orjson.dumps(
            {column: df[column].tolist() for column in df},
        )

    serialization_registry.register(
        "xarray_dataset",
        "application/json",
        serialize_json,
    )