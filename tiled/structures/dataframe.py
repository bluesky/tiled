from dataclasses import dataclass
from typing import List, Tuple, Union


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
        import pandas

        from ..serialization.dataframe import serialize_arrow

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
        import pandas

        from ..serialization.dataframe import serialize_arrow

        ddf = dask.dataframe.from_pandas(df, npartitions=1)
        meta = bytes(serialize_arrow(dask.dataframe.utils.make_meta(ddf), {}))
        divisions = bytes(
            serialize_arrow(pandas.DataFrame({"divisions": list(ddf.divisions)}), {})
        )
        return cls(meta=meta, divisions=divisions)

    @property
    def meta_decoded(self):
        from ..serialization.dataframe import deserialize_arrow

        return deserialize_arrow(self.meta)

    @property
    def divisions_decoded(self):
        from ..serialization.dataframe import deserialize_arrow

        divisions_wrapped_in_df = deserialize_arrow(self.divisions)
        return tuple(divisions_wrapped_in_df["divisions"].values)


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
        return cls(
            micro=DataFrameMicroStructure(**content["micro"]),
            macro=DataFrameMacroStructure(**content["macro"]),
        )
