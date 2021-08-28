import time

import dask.base
import dask.dataframe

from ..structures.dataframe import DataFrameMacroStructure, DataFrameMicroStructure
from ..utils import DictView
from ..server.internal_cache import get_internal_cache


class DataFrameAdapter:
    """
    Wrap a dataframe-like in a "Reader".

    Examples
    --------

    >>> df = pandas.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    >>> DataFrameAdapter(dask.dataframe.from_pandas(df), npartitions=1)

    Read a CSV (uses dask.dataframe.read_csv).

    >>> DataFrameAdapter.read_csv("myfiles.*.csv")
    >>> DataFrameAdapter.read_csv("s3://bucket/myfiles.*.csv")
    """

    structure_family = "dataframe"

    def __init__(self, data, metadata=None):
        self._metadata = metadata or {}
        if not isinstance(data, dask.dataframe.DataFrame):
            raise TypeError(
                f"data must be a dask.dataframe.Dataframe, not a {type(data)}"
            )
        self._data = data

    @classmethod
    def read_csv_no_cache(cls, *args, metadata=None, **kwargs):
        """
        Read a CSV and out opt of the internal (in-process) cache.

        Internally, this uses dask.dataframe.read_csv.
        It forward all parameters to that function. See
        https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.read_csv

        Examples
        --------

        >>> DataFrameAdapter.read_csv("myfiles.*.csv")
        >>> DataFrameAdapter.read_csv("s3://bucket/myfiles.*.csv")
        """
        return cls(dask.dataframe.read_csv(*args, **kwargs), metadata=metadata)

    @classmethod
    def read_csv(cls, *args, metadata=None, **kwargs):
        """
        Read a CSV.

        Internally, this uses dask.dataframe.read_csv.
        It forward all parameters to that function. See
        https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.read_csv

        Examples
        --------

        >>> DataFrameAdapter.read_csv("myfiles.*.csv")
        >>> DataFrameAdapter.read_csv("s3://bucket/myfiles.*.csv")
        """
        cache = get_internal_cache()
        if cache is None:
            return cls(dask.dataframe.read_csv(*args, **kwargs), metadata=metadata)
        else:
            cache_key = f"{cls.__module__}:{cls.__qualname__} {dask.base.tokenize((args, kwargs))}"
            df = cache.get(cache_key)
            if df is None:
                ddf = dask.dataframe.read_csv(*args, **kwargs)
                # A dask.dataframe does not know its byte size, so we use npartitions == 1
                # as a proxy for "small enough to cache".
                if ddf.npartitions == 1:
                    # Read the data now and cache it.
                    start_time = time.perf_counter()
                    df = ddf.compute()
                    cache.put(cache_key, df, cost=time.perf_counter() - start_time)
                    # Wrap the in-memory dataframe in dask for type stability.
                    return cls.from_pandas(df, npartitions=1, metadata=metadata)
                else:
                    # This is too large to cache. Defer reading the data.
                    return cls(ddf, metadata=metadata)
            else:
                # Return the cached result.
                # Wrap the in-memory dataframe in dask for type stability.
                return cls.from_pandas(df, npartitions=1, metadata=metadata)

    @classmethod
    def from_pandas(cls, *args, metadata=None, **kwargs):
        return cls(dask.dataframe.from_pandas(*args, **kwargs), metadata=metadata)

    read_csv.__doc__ = (
        """
    This wraps dask.dataframe.read_csv. Original docstring:

    """
        + dask.dataframe.read_csv.__doc__
    )

    def __repr__(self):
        return f"{type(self).__name__}({self._data!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        return DataFrameMacroStructure.from_dask_dataframe(self._data)

    def microstructure(self):
        return DataFrameMicroStructure.from_dask_dataframe(self._data)

    def read(self, columns=None):
        # TODO For the array reader we require returning a *lazy* object here.
        # Should rethink that. As is, this is inconsistent.
        # But we very intentionally do not support fancy row-slicing because
        # that becomes complex fast and it out of scope for Tiled.
        df = self._data
        if columns is not None:
            df = df[columns]
        return df.compute()

    def read_partition(self, partition, columns=None):
        partition = self._data.partitions[partition]
        if columns is not None:
            # Sub-select columns.
            partition = partition[columns]
        return partition.compute()

    def close(self):
        # Allow the garbage collector to reclaim this memory.
        self._data = None

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()
