import dask.base
import dask.dataframe
import pandas

from ..server.object_cache import NO_CACHE, get_object_cache
from ..structures.core import StructureFamily
from ..structures.dataframe import DataFrameStructure
from .array import ArrayAdapter


class DataFrameAdapter:
    """
    Wrap a dataframe-like object in an interface that Tiled can serve.

    Examples
    --------

    >>> df = pandas.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    >>> DataFrameAdapter.from_pandas(df, npartitions=1)

    Read a CSV (uses dask.dataframe.read_csv).

    >>> DataFrameAdapter.read_csv("myfiles.*.csv")
    >>> DataFrameAdapter.read_csv("s3://bucket/myfiles.*.csv")
    """

    structure_family = StructureFamily.dataframe

    @classmethod
    def from_pandas(
        cls, *args, metadata=None, specs=None, access_policy=None, **kwargs
    ):
        ddf = dask.dataframe.from_pandas(*args, **kwargs)
        return cls.from_dask_dataframe(
            ddf, metadata=metadata, specs=specs, access_policy=access_policy
        )

    @classmethod
    def from_dask_dataframe(
        cls,
        ddf,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        structure = DataFrameStructure.from_dask_dataframe(ddf)
        return cls(
            ddf.partitions,
            structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

    @classmethod
    def read_csv(
        cls,
        *args,
        arrow_schema=None,
        metadata=None,
        specs=None,
        access_policy=None,
        **kwargs,
    ):
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
        return cls.from_dask_dataframe(
            ddf, metadata=metadata, specs=specs, access_policy=access_policy
        )

    read_csv.__doc__ = (
        """
    This wraps dask.dataframe.read_csv. Original docstring:

    """
        + dask.dataframe.read_csv.__doc__
    )

    def __init__(
        self,
        partitions,
        structure,
        *,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        self._metadata = metadata or {}
        self._partitions = list(partitions)
        self._structure = structure
        self.specs = specs or []
        self.access_policy = access_policy

    def __repr__(self):
        return f"{type(self).__name__}({self._structure.columns!r})"

    def __getitem__(self, key):
        # Must compute to determine shape.
        return ArrayAdapter.from_array(self.read([key])[key].values)

    def items(self):
        yield from (
            (key, ArrayAdapter.from_array(self.read([key])[key].values))
            for key in self._structure.columns
        )

    def metadata(self):
        return self._metadata

    def structure(self):
        return self._structure

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
