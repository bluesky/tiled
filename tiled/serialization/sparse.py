import io

from ..media_type_registration import serialization_registry
from ..utils import modules_available

if modules_available("h5py"):

    def serialize_hdf5(sparse_arr, metadata):
        """
        Place coords and data in HDF5 datasets with those names.
        """
        import h5py

        buffer = io.BytesIO()
        with h5py.File(buffer, mode="w") as file:
            file.create_dataset("data", data=sparse_arr.data)
            file.create_dataset("coords", data=sparse_arr.coords)
            for k, v in metadata.items():
                file.attrs.create(k, v)
        return buffer.getbuffer()

    serialization_registry.register("sparse", "application/x-hdf5", serialize_hdf5)

if modules_available("pandas", "pyarrow"):
    import pandas

    from .dataframe import (
        APACHE_ARROW_FILE_MIME_TYPE,
        XLSX_MIME_TYPE,
        serialize_arrow,
        serialize_csv,
        serialize_excel,
        serialize_html,
        serialize_parquet,
    )

    # Support DataFrame formats by first converting to DataFrame.
    # naming columns like dim0, dim1, ..., dimN, data.
    def to_dataframe(sparse_arr):
        d = {f"dim{i}": coords for i, coords in enumerate(sparse_arr.coords)}
        d["data"] = sparse_arr.data
        return pandas.DataFrame(d)

    serialization_registry.register(
        "sparse",
        APACHE_ARROW_FILE_MIME_TYPE,
        lambda sparse_arr, metadata: serialize_arrow(
            to_dataframe(sparse_arr), metadata, preserve_index=False
        ),
    )
    serialization_registry.register(
        "sparse",
        "application/x-parquet",
        lambda sparse_arr, metadata: serialize_parquet(
            to_dataframe(sparse_arr), metadata, preserve_index=False
        ),
    )
    serialization_registry.register(
        "sparse",
        "text/csv",
        lambda sparse_arr, metadata: serialize_csv(
            to_dataframe(sparse_arr), metadata, preserve_index=False
        ),
    )
    serialization_registry.register(
        "sparse",
        "text/plain",
        lambda sparse_arr, metadata: serialize_csv(
            to_dataframe(sparse_arr), metadata, preserve_index=False
        ),
    )
    serialization_registry.register(
        "sparse",
        "text/html",
        lambda sparse_arr, metadata: serialize_html(
            to_dataframe(sparse_arr), metadata, preserve_index=False
        ),
    )
    serialization_registry.register(
        "sparse",
        XLSX_MIME_TYPE,
        lambda sparse_arr, metadata: serialize_excel(
            to_dataframe(sparse_arr), metadata, preserve_index=False
        ),
    )
    if modules_available("orjson"):
        import orjson

        def serialize_json(sparse_arr, metadata):
            df = to_dataframe(sparse_arr)
            return orjson.dumps(
                {column: df[column].tolist() for column in df},
            )

        serialization_registry.register(
            "sparse",
            "application/json",
            serialize_json,
        )
