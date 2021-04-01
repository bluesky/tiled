from dataclasses import dataclass
import mimetypes
import io
from typing import Any, List

import dask.dataframe.utils
import pandas
import pyarrow

from ..media_type_registration import serialization_registry, deserialization_registry
from ..utils import modules_available


@dataclass
class DataFrameMicroStructure:
    meta: pandas.DataFrame
    divisions: List[Any]

    @classmethod
    def from_dask_dataframe(cls, ddf):
        # Make an *empty* DataFrame with the same structure as ddf.
        # TODO Look at make_meta_nonempty to see if the "objects" are str or
        # datetime or actually generic objects.
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


def serialize_arrow(df):
    # pyarrow.serialize(...).to_buffer() returns a custom type.
    # Wrap it in memoryview so generic server code knows what to do with it.
    return memoryview(pyarrow.serialize(df).to_buffer())


def serialize_csv(df):
    file = io.BytesIO()
    df.to_csv(file)  # TODO How would we expose options in the server?
    return file.getbuffer()


def serialize_excel(df):
    file = io.BytesIO()
    df.to_excel(file)  # TODO How would we expose options in the server?
    return file.getbuffer()


def serialize_html(df):
    file = io.StringIO()
    df.to_html(file)  # TODO How would we expose options in the server?
    return file.getvalue().encode()


# The MIME type vnd.apache.arrow.file is provisional. See:
# https://lists.apache.org/thread.html/r9b462400a15296576858b52ae22e73f13c3e66f031757b2c9522f247%40%3Cdev.arrow.apache.org%3E  # noqa
# TODO Should we actually use vnd.apache.arrow.stream? I think 'file' is right
# for this use case but I have not read deeply into the details yet.
APACHE_ARROW_FILE_MIME_TYPE = "vnd.apache.arrow.file"
serialization_registry.register(
    "dataframe", APACHE_ARROW_FILE_MIME_TYPE, serialize_arrow
)
deserialization_registry.register(
    "dataframe", APACHE_ARROW_FILE_MIME_TYPE, pyarrow.deserialize
)
serialization_registry.register("dataframe", "text/csv", serialize_csv)
serialization_registry.register("dataframe", "text/html", serialize_html)
XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
if modules_available("openpyxl"):
    # The optional pandas dependency openpyxel is required for Excel read/write.
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
