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
from .node import walk


def as_dataset(self):
    import xarray

    data_vars = {}
    coords = {}
    for key, array_adapter in self.items():
        if "xarray_data_var" in array_adapter.specs:
            data_vars[key] = (
                array_adapter.macrostructure().dims,
                array_adapter.read(),
            )
        elif "xarray_coord" in array_adapter.specs:
            coords[key] = (
                array_adapter.macrostructure().dims,
                array_adapter.read(),
            )
    return xarray.Dataset(
        data_vars=data_vars, coords=coords, attrs=self.metadata["attrs"]
    )


class _BytesIOThatIgnoresClose(io.BytesIO):
    def close(self):
        # When the netcdf writer tells us to close(), ignore it.
        pass


if modules_available("scipy"):

    def serialize_netcdf(node, metadata):
        file = _BytesIOThatIgnoresClose()
        # Per the xarray.Dataset.to_netcdf documentation,
        # file-like objects are only supported by the scipy engine.
        as_dataset(node).to_netcdf(file, engine="scipy")
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
    lambda node, metadata: serialize_arrow(as_dataset(node).to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "application/x-parquet",
    lambda node, metadata: serialize_parquet(as_dataset(node).to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "text/csv",
    lambda node, metadata: serialize_csv(as_dataset(node).to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "text/x-comma-separated-values",
    lambda node, metadata: serialize_csv(as_dataset(node).to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "text/plain",
    lambda node, metadata: serialize_csv(as_dataset(node).to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    "text/html",
    lambda node, metadata: serialize_html(as_dataset(node).to_dataframe(), metadata),
)
serialization_registry.register(
    "xarray_dataset",
    XLSX_MIME_TYPE,
    lambda node, metadata: serialize_excel(as_dataset(node).to_dataframe(), metadata),
)
if modules_available("orjson"):
    import orjson

    def serialize_json(node, metadata):
        df = as_dataset(node).to_dataframe()
        return orjson.dumps(
            {column: df[column].tolist() for column in df},
        )

    serialization_registry.register(
        "xarray_dataset",
        "application/json",
        serialize_json,
    )
if modules_available("h5py"):

    def serialize_hdf5(node, metadata):
        """
        Like for node, but encode everything under 'attrs' in attrs.
        """
        import h5py

        buffer = io.BytesIO()
        root_node = node
        with h5py.File(buffer, mode="w") as file:
            for k, v in metadata["attrs"].items():
                file.attrs.create(k, v)
            for key_path, array_adapter in walk(node):
                group = file
                node = root_node
                for key in key_path[:-1]:
                    node = node[key]
                    if key in group:
                        group = group[key]
                    else:
                        group = group.create_group(key)
                        group.attrs.update(node.metadata["attrs"])
                data = array_adapter.read()
                dataset = group.create_dataset(key_path[-1], data=data)
                for k, v in array_adapter.metadata["attrs"].items():
                    dataset.attrs.create(k, v)
        return buffer.getbuffer()

    serialization_registry.register(
        "xarray_dataset", "application/x-hdf5", serialize_hdf5
    )
