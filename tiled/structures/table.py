import base64
import io
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, List, Tuple, Union

import pyarrow

from tiled.structures.root import Structure

B64_ENCODED_PREFIX = "data:application/vnd.apache.arrow.file;base64,"


def _uri_from_schema(pyarrow_schema: pyarrow.Schema) -> str:
    schema_bytes = pyarrow_schema.serialize()
    schema_b64 = base64.b64encode(schema_bytes).decode("utf-8")
    return B64_ENCODED_PREFIX + schema_b64


@dataclass
class TableStructure(Structure):
    # This holds a Arrow schema, base64-encoded so that it can be transported
    # as JSON. For clarity, the encoded data (...) is prefixed like:
    #
    # data:application/vnd.apache.arrow.file;base64,...
    #
    # Arrow does not support an official JSON serialization, but it
    # could in the future: https://github.com/apache/arrow/pull/7110
    # If it does, we could switch to using that here.
    arrow_schema: str
    npartitions: int
    columns: List[str]
    resizable: Union[bool, Tuple[bool, ...]] = False

    def __post_init__(self):
        self.columns = list(map(str, self.columns))  # Ensure all column names are str
        for column in self.columns:
            if column.startswith("_"):
                raise ValueError(
                    "Tiled reserved column names starting with '_' for internal use."
                )

    @classmethod
    def from_dask_dataframe(cls, ddf) -> "TableStructure":
        import dask.dataframe.utils

        # Make a pandas Table with 0 rows.
        # We can use this to define an Arrow schema without loading any row data.
        meta = dask.dataframe.utils.make_meta(ddf)
        schema = pyarrow.Table.from_pandas(meta).schema
        return cls(
            arrow_schema=_uri_from_schema(schema),
            npartitions=ddf.npartitions,
            columns=list(ddf.columns),
        )

    @classmethod
    def from_pandas(cls, df) -> "TableStructure":
        schema = pyarrow.Table.from_pandas(df).schema
        return cls(
            arrow_schema=_uri_from_schema(schema),
            npartitions=1,
            columns=list(df.columns),
        )

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "TableStructure":
        schema = pyarrow.Table.from_pydict(d).schema
        return cls(
            arrow_schema=_uri_from_schema(schema), npartitions=1, columns=list(d.keys())
        )

    @classmethod
    def from_arrays(cls, arr, names: List[str]) -> "TableStructure":
        schema = pyarrow.Table.from_arrays(arr, names).schema
        return cls(
            arrow_schema=_uri_from_schema(schema), npartitions=1, columns=list(names)
        )

    @classmethod
    def from_schema(
        cls, schema: pyarrow.Schema, npartitions: int = 1
    ) -> "TableStructure":
        return cls(
            arrow_schema=_uri_from_schema(schema),
            npartitions=npartitions,
            columns=schema.names,
        )

    @classmethod
    def from_arrow_table(
        cls, table: pyarrow.Table, npartitions: int = 1
    ) -> "TableStructure":
        schema = table.schema
        return cls(
            arrow_schema=_uri_from_schema(schema),
            npartitions=npartitions,
            columns=list(table.column_names),
        )

    @property
    def arrow_schema_decoded(self) -> pyarrow.Schema:
        if not self.arrow_schema.startswith(B64_ENCODED_PREFIX):
            raise ValueError(
                f"Expected base64-encoded data prefixed with {B64_ENCODED_PREFIX}."
            )

        payload = self.arrow_schema[len(B64_ENCODED_PREFIX) :].encode(  # noqa: 203
            "utf-8"
        )
        return pyarrow.ipc.read_schema(io.BytesIO(base64.b64decode(payload)))

    @property
    def meta(self):
        return self.arrow_schema_decoded.empty_table().to_pandas()
