from pathlib import Path
from urllib import parse

import dask.dataframe

from ..structures.core import StructureFamily
from .dataframe import DataFrameAdapter


class ParquetDatasetAdapter:
    structure_family = StructureFamily.dataframe

    def __init__(
        self,
        *partition_paths,
        meta,
        divisions,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        self.partition_paths = sorted(partition_paths)
        self.meta = meta
        self.divisions = divisions
        self.metadata = metadata or {}
        self.specs = list(specs or [])
        self.access_policy = access_policy

    @property
    def dataframe_adapter(self):
        partitions = []
        for path in self.partition_paths:
            if not Path(path).exists():
                partition = None
            else:
                partition = dask.dataframe.read_parquet(path)
            partitions.append(partition)
        return DataFrameAdapter(partitions, meta=self.meta, divisions=self.divisions)

    @classmethod
    def init_storage(cls, directory, npartitions):
        from ..server.schemas import Asset

        directory.mkdir()
        data_uri = parse.urlunparse(("file", "localhost", str(directory), "", "", None))
        assets = [
            Asset(
                data_uri=f"{data_uri}/partition-{i}.parquet",
                is_directory=False,
            )
            for i in range(npartitions)
        ]
        return assets

    def write_partition(self, data, partition):
        uri = self.partition_paths[partition]
        data.to_parquet(uri)

    def write(self, data):
        if self.macrostructure().npartitions != 1:
            raise NotImplementedError
        uri = self.partition_paths[0]
        data.to_parquet(uri)

    def read(self, *args, **kwargs):
        return self.dataframe_adapter.read(*args, **kwargs)

    def read_partition(self, *args, **kwargs):
        return self.dataframe_adapter.read_partition(*args, **kwargs)

    def macrostructure(self):
        return self.dataframe_adapter.macrostructure()

    def microstructure(self):
        return self.dataframe_adapter.microstructure()
