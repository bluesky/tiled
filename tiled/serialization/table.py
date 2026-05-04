import io
import mimetypes

import numpy
import pandas

from ..media_type_registration import (
    default_deserialization_registry,
    default_serialization_registry,
)
from ..structures.core import StructureFamily
from ..utils import (
    APACHE_ARROW_FILE_MIME_TYPE,
    XLSX_MIME_TYPE,
    modules_available,
    parse_mimetype,
)


@default_serialization_registry.register(
    StructureFamily.table, APACHE_ARROW_FILE_MIME_TYPE
)
def serialize_arrow(mimetype, df, metadata, preserve_index=True):
    import pyarrow

    if isinstance(df, pyarrow.Table):
        table = df
    elif isinstance(df, dict):
        table = pyarrow.Table.from_pydict(df)
    else:
        table = pyarrow.Table.from_pandas(df, preserve_index=preserve_index)
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return memoryview(sink.getvalue())


@default_deserialization_registry.register(
    StructureFamily.table, APACHE_ARROW_FILE_MIME_TYPE
)
def deserialize_arrow(buffer):
    import pyarrow

    return pyarrow.ipc.open_file(buffer).read_pandas()


# There seems to be no official Parquet MIME type.
# https://issues.apache.org/jira/browse/PARQUET-1889
@default_serialization_registry.register(StructureFamily.table, "application/x-parquet")
def serialize_parquet(mimetype, df, metadata, preserve_index=True):
    import pyarrow.parquet

    table = pyarrow.Table.from_pandas(df, preserve_index=preserve_index)
    sink = pyarrow.BufferOutputStream()
    with pyarrow.parquet.ParquetWriter(sink, table.schema) as writer:
        writer.write_table(table)
    return memoryview(sink.getvalue())


def serialize_csv(mimetype, df, metadata, preserve_index=False):
    file = io.StringIO()
    opt_params = parse_mimetype(mimetype)[1]
    include_header = opt_params.get("header", "present") != "absent"
    df.to_csv(file, header=include_header, index=preserve_index)
    return file.getvalue().encode()


@default_deserialization_registry.register(StructureFamily.table, "text/csv")
def deserialize_csv(mimetype, buffer):
    import pandas

    return pandas.read_csv(io.BytesIO(buffer), header=None)


default_serialization_registry.register(
    StructureFamily.table, "text/csv", serialize_csv
)
default_serialization_registry.register(
    StructureFamily.table, "text/x-comma-separated-values", serialize_csv
)
default_serialization_registry.register(
    StructureFamily.table, "text/plain", serialize_csv
)
default_serialization_registry.register(
    StructureFamily.table, "application/vnd.ms-excel", serialize_csv
)


@default_serialization_registry.register(StructureFamily.table, "text/html")
def serialize_html(mimetype, df, metadata, preserve_index=False):
    file = io.StringIO()
    df.to_html(file, index=preserve_index)
    return file.getvalue().encode()


if modules_available("openpyxl", "pandas"):
    # The optional pandas dependency openpyxel is required for Excel read/write.
    import pandas

    @default_serialization_registry.register(StructureFamily.table, XLSX_MIME_TYPE)
    def serialize_excel(mimetype, df, metadata, preserve_index=False):
        file = io.BytesIO()
        df.to_excel(file, index=preserve_index)
        return file.getbuffer()

    default_deserialization_registry.register(
        StructureFamily.table,
        XLSX_MIME_TYPE,
        lambda mimetype, buffer: pandas.read_excel(buffer),
    )
    mimetypes.types_map.setdefault(".xlsx", XLSX_MIME_TYPE)
if modules_available("orjson"):
    import orjson

    def _series_to_json_safe(series):
        """Convert a pandas Series to a list of JSON-serializable Python values.

        orjson is stricter than stdlib json: it rejects numpy scalars (float32,
        int32, ...), pandas NA, and pandas NaT. Convert all of these to their
        Python-native equivalents, with missing values becoming None.
        """
        arr = series.to_numpy(dtype=object, na_value=None)

        def to_native(v):
            if v is None:
                return None
            if isinstance(v, float) and numpy.isnan(v):
                return None
            if v is pandas.NaT:
                return None
            if isinstance(v, numpy.integer):
                return int(v)
            if isinstance(v, numpy.floating):
                return float(v)
            if isinstance(v, pandas.Timestamp):
                return v.isoformat()
            return v

        return [to_native(v) for v in arr]

    default_serialization_registry.register(
        StructureFamily.table,
        "application/json",
        lambda mimetype, df, metadata: orjson.dumps(
            {column: _series_to_json_safe(df[column]) for column in df},
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
    @default_serialization_registry.register(
        StructureFamily.table,
        "application/json-seq",  # official mimetype for newline-delimited JSON
    )
    def json_sequence(mimetype, df, metadata):
        # Build a JSON-safe version of the dataframe once, then emit row-by-row.
        safe = {column: _series_to_json_safe(df[column]) for column in df}
        n = len(df)
        if n == 0:
            yield b""
            return
        columns = list(safe)
        # First row has no leading newline; subsequent rows do.
        yield orjson.dumps({col: safe[col][0] for col in columns})
        for i in range(1, n):
            yield b"\n" + orjson.dumps({col: safe[col][i] for col in columns})


if modules_available("h5py"):
    from .container import serialize_hdf5

    default_serialization_registry.register(
        StructureFamily.table, "application/x-hdf5", serialize_hdf5
    )
