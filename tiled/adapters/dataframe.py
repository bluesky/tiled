import dask.base
import dask.dataframe
import pandas

from ..serialization.dataframe import serialize_arrow
from ..server.object_cache import NO_CACHE, get_object_cache
from ..structures.dataframe import DataFrameMacroStructure, DataFrameMicroStructure
from ..utils import DictView
from .array import ArrayAdapter


class DataFrameAdapter:
    """
    Wrap a dataframe-like object in an interface that Tiled can serve.

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
    def from_pandas(cls, *args, metadata=None, specs=None, **kwargs):
        ddf = dask.dataframe.from_pandas(*args, **kwargs)
        return cls.from_dask_dataframe(ddf, metadata=metadata, specs=specs)

    @classmethod
    def from_dask_dataframe(cls, ddf, metadata=None, specs=None):
        # Danger: using internal attribute _meta here.
        return cls(
            ddf.partitions, ddf._meta, ddf.divisions, metadata=metadata, specs=specs
        )

    @classmethod
    def read_csv(cls, *args, metadata=None, specs=None, **kwargs):
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
        return cls.from_dask_dataframe(ddf, metadata=metadata, specs=specs)

    read_csv.__doc__ = (
        """
    This wraps dask.dataframe.read_csv. Original docstring:

    """
        + dask.dataframe.read_csv.__doc__
    )

    def __init__(self, partitions, meta, divisions, *, metadata=None, specs=None):
        self._metadata = metadata or {}
        self._partitions = list(partitions)
        self._meta = meta
        self._divisions = divisions
        self.specs = specs or []

    def __repr__(self):
        return f"{type(self).__name__}({self._meta.columns!r})"

    def __getitem__(self, key):
        # Must compute to determine shape.
        return ArrayAdapter.from_array(self.read([key])[key].values)

    def items(self):
        yield from (
            (key, ArrayAdapter.from_array(self.read([key])[key].values))
            for key in self._meta.columns
        )

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        return DataFrameMacroStructure(
            columns=list(self._meta.columns), npartitions=len(self._partitions)
        )

    def microstructure(self):
        meta = bytes(serialize_arrow(self._meta, {}))
        divisions = bytes(
            serialize_arrow(pandas.DataFrame({"divisions": list(self._divisions)}), {})
        )
        return DataFrameMicroStructure(meta=meta, divisions=divisions)

    def read(self, fields=None):
        if any(p is None for p in self._partitions):
            raise ValueError("Not all partitions have been stored.")
        if isinstance(self._partitions[0], dask.dataframe.DataFrame):
            if fields is not None:
                ddf = dask.dataframe.concat(
                    [p[fields] for p in self._partitions], axis=0
                )
            else:
                ddf = dask.dataframe.concat(self._partitions, axis=0)
            # Note: If the cache is set to NO_CACHE, this is a null context.
            with get_object_cache().dask_context:
                return ddf.compute()
        df = pandas.concat(self._partitions, axis=0)
        if fields is not None:
            df = df[fields]
        return df

    def read_partition(self, partition, fields=None):
        partition = self._partitions[partition]
        if partition is None:
            raise RuntimeError(f"partition {partition} has not be stored yet")
        if fields is not None:
            partition = partition[fields]
        # Special case for dask to cache computed result in object cache.
        if isinstance(partition, dask.dataframe.DataFrame):
            # Note: If the cache is set to NO_CACHE, this is a null context.
            with get_object_cache().dask_context:
                return partition.compute()
        return partition
