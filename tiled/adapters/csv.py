from pathlib import Path

import dask.dataframe

from ..server.object_cache import NO_CACHE, get_object_cache
from ..structures.core import StructureFamily
from ..structures.table import TableStructure
from ..utils import path_from_uri
from .dataframe import DataFrameAdapter


def read_csv(
    data_uri,
    structure=None,
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

    >>> read_csv("myfiles.*.csv")
    >>> read_csv("s3://bucket/myfiles.*.csv")
    """
    filepath = path_from_uri(data_uri)
    ddf = dask.dataframe.read_csv(filepath, **kwargs)
    # If an instance has previously been created using the same parameters,
    # then we are here because the caller wants a *fresh* view on this data.
    # Therefore, we should clear any cached data.
    cache = get_object_cache()
    if cache is not NO_CACHE:
        cache.discard_dask(ddf.__dask_keys__())
    # TODO Pass structure through rather than just re-creating it
    # in from_dask_dataframe.
    return DataFrameAdapter.from_dask_dataframe(
        ddf, metadata=metadata, specs=specs, access_policy=access_policy
    )


read_csv.__doc__ = """
This wraps dask.dataframe.read_csv. Original docstring:

""" + (
    dask.dataframe.read_csv.__doc__ or ""
)


class CSVAdapter:
    structure_family = StructureFamily.table

    def __init__(
        self,
        data_uris,
        structure,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        # TODO Store data_uris instead and generalize to non-file schemes.
        self._partition_paths = [path_from_uri(uri) for uri in data_uris]
        self._metadata = metadata or {}
        if structure is None:
            table = dask.dataframe.read_csv(self._partition_paths)
            structure = TableStructure.from_dask_dataframe(table)
        self._structure = structure
        self.specs = list(specs or [])
        self.access_policy = access_policy

    def metadata(self):
        return self._metadata

    @property
    def dataframe_adapter(self):
        partitions = []
        for path in self._partition_paths:
            if not Path(path).exists():
                partition = None
            else:
                partition = dask.dataframe.read_csv(path)
            partitions.append(partition)
        return DataFrameAdapter(partitions, self._structure)

    @classmethod
    def init_storage(cls, data_uri, structure):
        from ..server.schemas import Asset

        directory = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        assets = [
            Asset(
                data_uri=f"{data_uri}/partition-{i}.csv",
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
            for i in range(structure.npartitions)
        ]
        return assets

    def append_partition(self, data, partition):
        uri = self._partition_paths[partition]
        data.to_csv(uri, index=False, mode="a", header=False)

    def write_partition(self, data, partition):
        uri = self._partition_paths[partition]
        data.to_csv(uri, index=False)

    def write(self, data):
        if self.structure().npartitions != 1:
            raise NotImplementedError
        uri = self._partition_paths[0]
        data.to_csv(uri, index=False)

    def read(self, *args, **kwargs):
        return self.dataframe_adapter.read(*args, **kwargs)

    def read_partition(self, *args, **kwargs):
        return self.dataframe_adapter.read_partition(*args, **kwargs)

    def structure(self):
        return self._structure

    def get(self, key):
        return self.dataframe_adapter.get(key)
