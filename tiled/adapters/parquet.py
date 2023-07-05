from pathlib import Path

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
    ):
        self.partition_paths = sorted(partition_paths)
        self.meta = meta
        self.divisions = divisions
        self.metadata = metadata or {}
        self.specs = list(specs or [])

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

        Path(directory).mkdir()
        assets = [
            Asset(
                data_uri=f"file://localhost{Path(directory).absolute()}/partition-{i}.parquet",
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
