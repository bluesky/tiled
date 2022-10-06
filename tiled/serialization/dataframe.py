import io
import mimetypes

from ..media_type_registration import deserialization_registry, serialization_registry
from ..utils import APACHE_ARROW_FILE_MIME_TYPE, XLSX_MIME_TYPE, modules_available


def serialize_arrow(df, metadata, preserve_index=True):
    import pyarrow

    table = pyarrow.Table.from_pandas(df, preserve_index=preserve_index)
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return memoryview(sink.getvalue())


def deserialize_arrow(buffer):
    import pyarrow

    return pyarrow.ipc.open_file(buffer).read_pandas()


def serialize_parquet(df, metadata, preserve_index=True):
    import pyarrow.parquet

    table = pyarrow.Table.from_pandas(df, preserve_index=preserve_index)
    sink = pyarrow.BufferOutputStream()
    with pyarrow.parquet.ParquetWriter(sink, table.schema) as writer:
        writer.write_table(table)
    return memoryview(sink.getvalue())


def serialize_csv(df, metadata, preserve_index=False):
    file = io.StringIO()
    df.to_csv(file, index=preserve_index)
    return file.getvalue().encode()


def serialize_excel(df, metadata, preserve_index=False):
    file = io.BytesIO()
    df.to_excel(file, index=preserve_index)
    return file.getbuffer()


def serialize_html(df, metadata, preserve_index=False):
    file = io.StringIO()
    df.to_html(file, index=preserve_index)
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
serialization_registry.register(
    "dataframe", "text/x-comma-separated-values", serialize_csv
)
serialization_registry.register("dataframe", "text/plain", serialize_csv)
serialization_registry.register("dataframe", "text/html", serialize_html)
if modules_available("openpyxl", "pandas"):
    # The optional pandas dependency openpyxel is required for Excel read/write.
    import pandas

    serialization_registry.register("dataframe", XLSX_MIME_TYPE, serialize_excel)
    deserialization_registry.register("dataframe", XLSX_MIME_TYPE, pandas.read_excel)
    mimetypes.types_map.setdefault(".xlsx", XLSX_MIME_TYPE)
if modules_available("orjson"):
    import orjson

    serialization_registry.register(
        "dataframe",
        "application/json",
        lambda df, metadata: orjson.dumps(
            {column: df[column].tolist() for column in df},
        ),
    )

    # Newline-delimited JSON. For example, this DataFrame:
    #
    # >>> pandas.DataFrame({"a": [1,2,3], "b": [4,5,6]})
    #
    # renders as this multi-line output:
    #
    # {'a': 1, 'b': 4}
    # {'a': 2, 'b': 5}
    # {'a': 3, 'b': 6}
    def json_sequence(df, metadata):
        rows = df.iterrows()
        # The first row is a special case; the rest start with a newline.
        try:
            # Emit the first row with no newline.
            _, row = next(rows)
        except StopIteration:
            # No rows
            yield b""
        else:
            # Emit the remaining rows, prepending newline.
            yield orjson.dumps(row.to_dict())
            for _, row in rows:
                yield b"\n" + orjson.dumps(row.to_dict())

    serialization_registry.register(
        "dataframe",
        "application/json-seq",  # official mimetype for newline-delimited JSON
        json_sequence,
    )

if modules_available("h5py"):
    from .node import serialize_hdf5

    serialization_registry.register("dataframe", "application/x-hdf5", serialize_hdf5)
