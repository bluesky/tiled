from dataclasses import dataclass
import mimetypes
import io
from typing import Any, List

from ..media_type_registration import serialization_registry, deserialization_registry
from ..utils import APACHE_ARROW_FILE_MIME_TYPE, XLSX_MIME_TYPE, modules_available


@dataclass
class DataFrameMicroStructure:
    meta: "pandas.DataFrame"
    divisions: List[Any]

    @classmethod
    def from_dask_dataframe(cls, ddf):
        # Make an *empty* DataFrame with the same structure as ddf.
        # TODO Look at make_meta_nonempty to see if the "objects" are str or
        # datetime or actually generic objects.
        import dask.dataframe.utils

        meta = dask.dataframe.utils.make_meta(ddf)
        return cls(meta=meta, divisions=ddf.divisions)


@dataclass
class DataFrameMacroStructure:
    npartitions: int
    columns: List[str]

    @classmethod
    def from_dask_dataframe(cls, ddf):
        return cls(npartitions=ddf.npartitions, columns=list(ddf.columns))


@dataclass
class DataFrameStructure:
    micro: DataFrameMicroStructure
    macro: DataFrameMacroStructure


def serialize_arrow(df, metadata):
    import pyarrow

    table = pyarrow.Table.from_pandas(df)
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return memoryview(sink.getvalue())


def deserialize_arrow(buffer):
    import pyarrow

    return pyarrow.ipc.open_file(buffer).read_pandas()


def serialize_parquet(df, metadata):
    import pyarrow.parquet

    table = pyarrow.Table.from_pandas(df)
    sink = pyarrow.BufferOutputStream()
    with pyarrow.parquet.ParquetWriter(sink, table.schema) as writer:
        writer.write_table(table)
    return memoryview(sink.getvalue())


def serialize_csv(df, metadata):
    file = io.BytesIO()
    df.to_csv(file)  # TODO How would we expose options in the server?
    return file.getbuffer()


def serialize_excel(df, metadata):
    file = io.BytesIO()
    df.to_excel(file)  # TODO How would we expose options in the server?
    return file.getbuffer()


def serialize_html(df, metadata):
    file = io.StringIO()
    df.to_html(file)  # TODO How would we expose options in the server?
    return file.getvalue().encode()


serialization_registry.register(
    "dataframe", APACHE_ARROW_FILE_MIME_TYPE, serialize_arrow
)
deserialization_registry.register(
    "dataframe", APACHE_ARROW_FILE_MIME_TYPE, deserialize_arrow
)
# There seems to be no official Parquet MIME type.
# https://issues.apache.org/jira/browse/PARQUET-1889
serialization_registry.register("dataframe", "application/x-parquet", serialize_parquet)
serialization_registry.register("dataframe", "text/csv", serialize_csv)
serialization_registry.register("dataframe", "text/plain", serialize_csv)
serialization_registry.register("dataframe", "text/html", serialize_html)
if modules_available("openpyxl", "pandas"):
    # The optional pandas dependency openpyxel is required for Excel read/write.
    import pandas

    serialization_registry.register(
        "dataframe",
        XLSX_MIME_TYPE,
        serialize_excel,
    )
    deserialization_registry.register(
        "dataframe",
        XLSX_MIME_TYPE,
        pandas.read_excel,
    )
    mimetypes.types_map.setdefault(".xlsx", XLSX_MIME_TYPE)
