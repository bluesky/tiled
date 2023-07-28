import base64
import io
from typing import List, Tuple, Union

from pydantic import BaseModel

from ..structures.dataframe import B64_ENCODED_PREFIX


class DataFrameMicroStructure(BaseModel):
    # This holds a Arrow schema, base64-encoded so that it can be transported
    # as JSON. For clarity, the encoded data (...) is prefixed like:
    #
    # data:application/vnd.apache.arrow.file;base64,...
    #
    # Arrow does not support an official JSON serialization, but it
    # could in the future: https://github.com/apache/arrow/pull/7110
    # If it does, we could switch to using that here.
    arrow_schema: str

    @classmethod
    def from_dask_dataframe(cls, ddf):
        import dask.dataframe.utils
        import pyarrow

        # Make a pandas DataFrame with 0 rows.
        # We can use this to define an Arrow schema without loading any row data.
        meta = dask.dataframe.utils.make_meta(ddf)
        schema_bytes = pyarrow.Table.from_pandas(meta).schema.serialize()
        schema_b64 = base64.b64encode(schema_bytes).decode("utf-8")
        data_uri = B64_ENCODED_PREFIX + schema_b64
        return cls(arrow_schema=data_uri)

    @classmethod
    def from_pandas(cls, df):
        import pyarrow

        schema_bytes = pyarrow.Table.from_pandas(df).schema.serialize()
        schema_b64 = base64.b64encode(schema_bytes).decode("utf-8")
        data_uri = B64_ENCODED_PREFIX + schema_b64
        return cls(arrow_schema=data_uri)

    @property
    def arrow_schema_decoded(self):
        import pyarrow

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


class DataFrameMacroStructure(BaseModel):
    npartitions: int
    columns: List[str]
    resizable: Union[bool, Tuple[bool, ...]] = False

    @classmethod
    def from_dask_dataframe(cls, ddf):
        return cls(npartitions=ddf.npartitions, columns=list(ddf.columns))


class DataFrameStructure(BaseModel):
    micro: DataFrameMicroStructure
    macro: DataFrameMacroStructure

    @classmethod
    def from_json(cls, content):
        return cls(
            micro=DataFrameMicroStructure(**content["micro"]),
            macro=DataFrameMacroStructure(**content["macro"]),
        )
