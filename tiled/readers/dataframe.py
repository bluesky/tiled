import dask.base
import dask.dataframe

from ..structures.dataframe import DataFrameMacroStructure, DataFrameMicroStructure
from ..utils import DictView
from ..server.object_cache import get_object_cache, NO_CACHE


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
        ddf = dask.dataframe.read_csv(*args, **kwargs)
        # If an instance has previously been created using the same parameters,
        # then we are here because the caller wants a *fresh* view on this data.
        # Therefore, we should clear any cached data.
        cache = get_object_cache()
        if cache is not NO_CACHE:
            cache.discard_dask(ddf.__dask_keys__())
        return cls(ddf, metadata=metadata)

    read_csv.__doc__ = (
        """
    This wraps dask.dataframe.read_csv. Original docstring:

    """
        + dask.dataframe.read_csv.__doc__
    )

    def __init__(self, ddf, metadata=None):
        self._metadata = metadata or {}
        self._ddf = ddf

    def __repr__(self):
        return f"{type(self).__name__}({self._ddf!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        return DataFrameMacroStructure.from_dask_dataframe(self._ddf)

    def microstructure(self):
        return DataFrameMicroStructure.from_dask_dataframe(self._ddf)

    def read(self, columns=None):
        # TODO For the array reader we require returning a *lazy* object here.
        # Should rethink that. As is, this is inconsistent.
        # But we very intentionally do not support fancy row-slicing because
        # that becomes complex fast and it out of scope for Tiled.
        ddf = self._ddf
        if columns is not None:
            ddf = ddf[columns]
        # Note: If the cache is set to NO_CACHE, this is a null context.
        with get_object_cache().dask_context:
            return ddf.compute()

    def read_partition(self, partition, columns=None):
        partition = self._ddf.partitions[partition]
        if columns is not None:
            # Sub-select columns.
            partition = partition[columns]
        # Note: If the cache is set to NO_CACHE, this is a null context.
        with get_object_cache().dask_context:
            return partition.compute()
