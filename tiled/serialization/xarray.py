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


def as_dataset(node):
    import xarray

    data_vars = {}
    coords = {}
    for key, array_adapter in node.items():
        spec_names = set(spec.name for spec in array_adapter.specs)
        if "xarray_data_var" in spec_names:
            data_vars[key] = (
                array_adapter.macrostructure().dims,
                array_adapter.read(),
            )
        elif "xarray_coord" in spec_names:
            coords[key] = (
                array_adapter.macrostructure().dims,
                array_adapter.read(),
            )
        else:
            raise ValueError(
                "Child nodes of xarray_dataset should include spec "
                "'xarray_coord' or 'xarray_data_var'."
            )
    return xarray.Dataset(
        data_vars=data_vars, coords=coords, attrs=node.metadata["attrs"]
    )


class _BytesIOThatIgnoresClose(io.BytesIO):
    def close(self):
        # When the netcdf writer tells us to close(), ignore it.
        pass


if modules_available("scipy"):

    def serialize_netcdf(node, metadata, filter_for_access):
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
    lambda node, metadata, filter_for_access: serialize_arrow(
        as_dataset(node).to_dataframe(), metadata
    ),
)
serialization_registry.register(
    "xarray_dataset",
    "application/x-parquet",
    lambda node, metadata, filter_for_access: serialize_parquet(
        as_dataset(node).to_dataframe(), metadata
    ),
)
serialization_registry.register(
    "xarray_dataset",
    "text/csv",
    lambda node, metadata, filter_for_access: serialize_csv(
        as_dataset(node).to_dataframe(), metadata
    ),
)
serialization_registry.register(
    "xarray_dataset",
    "text/x-comma-separated-values",
    lambda node, metadata, filter_for_access: serialize_csv(
        as_dataset(node).to_dataframe(), metadata
    ),
)
serialization_registry.register(
    "xarray_dataset",
    "text/plain",
    lambda node, metadata, filter_for_access: serialize_csv(
        as_dataset(node).to_dataframe(), metadata
    ),
)
serialization_registry.register(
    "xarray_dataset",
    "text/html",
    lambda node, metadata, filter_for_access: serialize_html(
        as_dataset(node).to_dataframe(), metadata
    ),
)
serialization_registry.register(
    "xarray_dataset",
    XLSX_MIME_TYPE,
    lambda node, metadata, filter_for_access: serialize_excel(
        as_dataset(node).to_dataframe(), metadata
    ),
)
if modules_available("orjson"):
    import orjson

    def serialize_json(node, metadata, filter_for_access):
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

    async def serialize_hdf5(node, metadata, filter_for_access):
        """
        Like for node, but encode everything under 'attrs' in attrs.
        """
        import h5py

        buffer = io.BytesIO()
        root_node = node
        with h5py.File(buffer, mode="w") as file:
            for k, v in metadata["attrs"].items():
                file.attrs.create(k, v)
            async for key_path, array_adapter in walk(node, filter_for_access):
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
