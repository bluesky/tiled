from dataclasses import dataclass
from typing import List, Tuple, Union

import pandas

from ..serialization.dataframe import deserialize_arrow, serialize_arrow


@dataclass
class DataFrameMicroStructure:
    meta: bytes  # Arrow-encoded DataFrame
    divisions: bytes  # Arrow-encoded DataFrame

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


@dataclass
class DataFrameMacroStructure:
    npartitions: int
    columns: List[str]
    resizable: Union[bool, Tuple[bool, ...]] = False

    @classmethod
    def from_dask_dataframe(cls, ddf):
        return cls(npartitions=ddf.npartitions, columns=list(ddf.columns))


@dataclass
class DataFrameStructure:
    micro: DataFrameMicroStructure
    macro: DataFrameMacroStructure

    @classmethod
    def from_json(cls, content):
        divisions_wrapped_in_df = deserialize_arrow(content["micro"]["divisions"])
        divisions = tuple(divisions_wrapped_in_df["divisions"].values)
        meta = deserialize_arrow(content["micro"]["meta"])
        return cls(
            micro=DataFrameMicroStructure(meta=meta, divisions=divisions),
            macro=DataFrameMacroStructure(**content["macro"]),
        )
