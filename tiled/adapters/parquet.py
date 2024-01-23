from pathlib import Path

import dask.dataframe

from ..structures.core import StructureFamily
from ..utils import path_from_uri
from .dataframe import DataFrameAdapter


class ParquetDatasetAdapter:
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
                partition = dask.dataframe.read_parquet(path)
            partitions.append(partition)
        return DataFrameAdapter(partitions, self._structure)

    @classmethod
    def init_storage(cls, data_uri, structure):
        from ..server.schemas import Asset

        directory = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        assets = [
            Asset(
                data_uri=f"{data_uri}/partition-{i}.parquet",
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
            for i in range(structure.npartitions)
        ]
        return assets

    def write_partition(self, data, partition):
        uri = self._partition_paths[partition]
        data.to_parquet(uri)

    def write(self, data):
        if self.structure().npartitions != 1:
            raise NotImplementedError
        uri = self._partition_paths[0]
        data.to_parquet(uri)

    def read(self, *args, **kwargs):
        return self.dataframe_adapter.read(*args, **kwargs)

    def read_partition(self, *args, **kwargs):
        return self.dataframe_adapter.read_partition(*args, **kwargs)

    def structure(self):
        return self._structure
