from typing import Any, List, Tuple, TypeVar, Union

from pydantic import BaseModel

from ..media_type_registration import deserialization_registry
from ..utils import APACHE_ARROW_FILE_MIME_TYPE

# import pandas

PandasDataFrame = TypeVar("pandas.core.frame.DataFrame")


class DataFrameMicroStructure(BaseModel):
    # meta: "pandas.DataFrame"
    meta: PandasDataFrame
    divisions: List[Any]

    @classmethod
    def from_dask_dataframe(cls, ddf):
        # Make an *empty* DataFrame with the same structure as ddf.
        # TODO Look at make_meta_nonempty to see if the "objects" are str or
        # datetime or actually generic objects.
        import dask.dataframe.utils

        meta = dask.dataframe.utils.make_meta(ddf)
        return cls(meta=meta, divisions=ddf.divisions)


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
