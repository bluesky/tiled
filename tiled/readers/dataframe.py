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

    @classmethod
    def from_pandas(cls, *args, metadata=None, **kwargs):
        return cls(dask.dataframe.from_pandas(*args, **kwargs), metadata=metadata)

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
        return cls(lambda: dask.dataframe.read_csv(*args, **kwargs), metadata=metadata)

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
        ddf = dask.dataframe.read_csv(*args, **kwargs)
        # A dask.dataframe does not know its byte size, so we use npartitions == 1
        # as a proxy for "small enough to cache".
        if (cache is not None) and ddf.npartitions == 1:
            # Read the data now and cache it.
            cache_key = f"{cls.__module__}:{cls.__qualname__}-{dask.base.tokenize((args, kwargs))}"
            start_time = time.perf_counter()
            df = ddf.compute()
            cache.put(cache_key, df, cost=time.perf_counter() - start_time)
            try_the_cache = True
        else:
            try_the_cache = False

        def data_factory():
            if not try_the_cache:
                return ddf
            # Try to load the data from the cache.
            df = cache.get(cache_key)
            if df is None:
                # It has been evicted. Cache it again.
                start_time = time.perf_counter()
                df = ddf.compute()
                cache.put(cache_key, df, cost=time.perf_counter() - start_time)
            return dask.dataframe.from_pandas(df, npartitions=1)

        return cls(data_factory, metadata=metadata)

    read_csv.__doc__ = (
        """
    This wraps dask.dataframe.read_csv. Original docstring:

    """
        + dask.dataframe.read_csv.__doc__
    )

    read_csv_no_cache.__doc__ = (
        """
    This wraps dask.dataframe.read_csv. Original docstring:

    """
        + dask.dataframe.read_csv.__doc__
    )

    def __init__(self, data_factory, metadata=None):
        self._metadata = metadata or {}
        self._data_factory = data_factory

    def __repr__(self):
        return f"{type(self).__name__}({self._data_factory()!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        return DataFrameMacroStructure.from_dask_dataframe(self._data_factory())

    def microstructure(self):
        return DataFrameMicroStructure.from_dask_dataframe(self._data_factory())

    def read(self, columns=None):
        # TODO For the array reader we require returning a *lazy* object here.
        # Should rethink that. As is, this is inconsistent.
        # But we very intentionally do not support fancy row-slicing because
        # that becomes complex fast and it out of scope for Tiled.
        df = self._data_factory()
        if columns is not None:
            df = df[columns]
        return df.compute()

    def read_partition(self, partition, columns=None):
        partition = self._data_factory().partitions[partition]
        if columns is not None:
            # Sub-select columns.
            partition = partition[columns]
        return partition.compute()
