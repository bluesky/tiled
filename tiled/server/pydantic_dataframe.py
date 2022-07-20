from typing import List, Tuple, Union

import pandas
from pydantic import BaseModel

from ..media_type_registration import deserialization_registry
from ..serialization.dataframe import serialize_arrow
from ..utils import APACHE_ARROW_FILE_MIME_TYPE


class DataFrameMicroStructure(BaseModel):
    meta: bytes
    divisions: bytes

    @classmethod
    def from_dask_dataframe(cls, ddf):
        # Make an *empty* DataFrame with the same structure as ddf.
        # TODO Look at make_meta_nonempty to see if the "objects" are str or
        # datetime or actually generic objects.
        import dask.dataframe.utils

        meta = bytes(serialize_arrow(dask.dataframe.utils.make_meta(ddf), {}))
        divisions = bytes(
            serialize_arrow(pandas.DataFrame({"divisions": list(ddf.divisions)}), {})
        )
        return cls(meta=meta, divisions=divisions)

    @classmethod
    def from_dataframe(cls, df):
        # Make an *empty* DataFrame with the same structure as ddf.
        # TODO Look at make_meta_nonempty to see if the "objects" are str or
        # datetime or actually generic objects.
        import dask.dataframe
        import dask.dataframe.utils

        ddf = dask.dataframe.from_pandas(df, npartitions=1)
        meta = bytes(serialize_arrow(dask.dataframe.utils.make_meta(ddf), {}))
        divisions = bytes(
            serialize_arrow(pandas.DataFrame({"divisions": list(ddf.divisions)}), {})
        )
        return cls(meta=meta, divisions=divisions)


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
        divisions_wrapped_in_df = deserialization_registry(
            "dataframe", APACHE_ARROW_FILE_MIME_TYPE, content["micro"]["divisions"]
        )
        divisions = tuple(divisions_wrapped_in_df["divisions"].values)
        meta = deserialization_registry(
            "dataframe", APACHE_ARROW_FILE_MIME_TYPE, content["micro"]["meta"]
        )
        return cls(
            micro=DataFrameMicroStructure(meta=meta, divisions=divisions),
            macro=DataFrameMacroStructure(**content["macro"]),
        )
