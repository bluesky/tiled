from pathlib import Path

import dask.dataframe

from .dataframe import DataFrameAdapter


class ParquetDatasetAdapter:

    def __init__(self, partition_uris, meta, divisions, metadata=None, specs=None, references=None):
        self.partition_uris = partition_uris
        self.meta = meta
        self.divisions = divisions
        self.metadata = metadata or {}
        self.specs = list(specs or [])
        self.references = list(references or [])

    @property
    def dataframe_adapter(self):
        partitions = []
        for uri in self.partition_uris:
            # TODO Set partition = None if file does not exist (yet).
            partition = dask.dataframe.read_parquet(uri)
            partitions.append(partition)
        return DataFrameAdapter(partitions, meta=self.meta, divisions=self.divisions)

    @classmethod
    def init_storage(cls, directory, npartitions):
        from ..server.schemas import Asset

        Path(directory).mkdir()
        return [
            Asset(
                data_uri=f"file://{Path(directory).absolute()}/partition-{i}",
                is_directory=False,
            )
            for i in range(npartitions)
        ]

    def write_partition(self, data, partition):
        uri = self.partition_uris[partition]
        data.to_parquet(uri)

    def write(self, data):
        if self.macrostructure().npartitions != 1:
            raise NotImplementedError
        uri = self.partition_uris[0]
        data.to_parquet(uri)

    def read(self, *args, **kwargs):
        return self.dataframe_adapter.read(*args, **kwargs)

    def read_partition(self, *args, **kwargs):
        return self.dataframe_adapter.read_partition(*args, **kwargs)

    def macrostructure(self):
        return self.dataframe_adapter.macrostructure()

    def microstructure(self):
        return self.dataframe_adapter.microstructure()

