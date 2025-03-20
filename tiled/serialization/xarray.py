import io

from ..media_type_registration import default_serialization_registry
from ..utils import ensure_awaitable, modules_available
from .container import walk
from .table import (
    APACHE_ARROW_FILE_MIME_TYPE,
    XLSX_MIME_TYPE,
    serialize_arrow,
    serialize_csv,
    serialize_excel,
    serialize_html,
    serialize_parquet,
)


async def as_dataset(node):
    import xarray

    data_vars = {}
    coords = {}
    if hasattr(node, "items_range"):
        items = await node.items_range(0, None)
    else:
        items = node.items()
    for key, array_adapter in items:
        spec_names = set(spec.name for spec in array_adapter.specs)
        arr = await ensure_awaitable(array_adapter.read)
        if "xarray_data_var" in spec_names:
            data_vars[key] = (array_adapter.structure().dims, arr)
        elif "xarray_coord" in spec_names:
            coords[key] = (array_adapter.structure().dims, arr)
        else:
            raise ValueError(
                "Child nodes of xarray_dataset should include spec "
                "'xarray_coord' or 'xarray_data_var'."
            )
    return xarray.Dataset(
        data_vars=data_vars,
        coords=coords,
        attrs=getattr(node, "metadata", lambda: {})().get("attrs"),
    )


if modules_available("scipy"):
    # Both application/netcdf and application/x-netcdf are used.
    # https://en.wikipedia.org/wiki/NetCDF
    @default_serialization_registry.register(
        "xarray_dataset", ["application/netcdf", "application/x-netcdf"]
    )
    async def serialize_netcdf(node, metadata, filter_for_access=None):
        # Per the xarray.Dataset.to_netcdf documentation,
        # return a byte stream, which is only supported by the scipy engine.
        return (await as_dataset(node)).to_netcdf(path=None, engine="scipy")


# Support DataFrame formats by first converting to DataFrame.
# This doesn't make much sense for N-dimensional variables, but for
# 1-dimensional variables it is useful.


@default_serialization_registry.register("xarray_dataset", APACHE_ARROW_FILE_MIME_TYPE)
async def serialize_dataset_arrow(node, metadata, filter_for_access=None):
    return serialize_arrow((await as_dataset(node)).to_dataframe(), metadata)


@default_serialization_registry.register("xarray_dataset", "application/x-parquet")
async def serialize_dataset_parquet(node, metadata, filter_for_access=None):
    return serialize_parquet((await as_dataset(node)).to_dataframe(), metadata)


@default_serialization_registry.register(
    "xarray_dataset", ["text/csv", "text/comma-separated-values", "text/plain"]
)
async def serialize_dataset_csv(node, metadata, filter_for_access=None):
    return serialize_csv((await as_dataset(node)).to_dataframe(), metadata)


@default_serialization_registry.register("xarray_dataset", "text/html")
async def serialize_dataset_html(node, metadata, filter_for_access=None):
    return serialize_html((await as_dataset(node)).to_dataframe(), metadata)


@default_serialization_registry.register("xarray_dataset", XLSX_MIME_TYPE)
async def serialize_dataset_excel(node, metadata, filter_for_access=None):
    return serialize_excel((await as_dataset(node)).to_dataframe(), metadata)


if modules_available("orjson"):
    import orjson

    @default_serialization_registry.register("xarray_dataset", "application/json")
    async def serialize_json(node, metadata, filter_for_access=None):
        df = (await as_dataset(node)).to_dataframe()
        return orjson.dumps(
            {column: df[column].tolist() for column in df},
        )


if modules_available("h5py"):

    @default_serialization_registry.register("xarray_dataset", "application/x-hdf5")
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
                        group.attrs.update(node.metadata()["attrs"])
                data = array_adapter.read()
                dataset = group.create_dataset(key_path[-1], data=data)
                for k, v in array_adapter.metadata()["attrs"].items():
                    dataset.attrs.create(k, v)
        return buffer.getbuffer()
