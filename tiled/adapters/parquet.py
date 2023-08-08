from pathlib import Path
from urllib import parse

import dask.dataframe

from ..structures.core import StructureFamily
from .dataframe import DataFrameAdapter


class ParquetDatasetAdapter:
    structure_family = StructureFamily.table

    def __init__(
        self,
        *partition_paths,
        structure,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        self.partition_paths = sorted(partition_paths)
        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])
        self.access_policy = access_policy

    def metadata(self):
        return self._metadata

    @property
    def dataframe_adapter(self):
        partitions = []
        for path in self.partition_paths:
            if not Path(path).exists():
                partition = None
            else:
                partition = dask.dataframe.read_parquet(path)
            partitions.append(partition)
        return DataFrameAdapter(partitions, self._structure)

    @classmethod
    def init_storage(cls, directory, structure):
        from ..server.schemas import Asset

        directory.mkdir()
        data_uri = parse.urlunparse(("file", "localhost", str(directory), "", "", None))
        assets = [
            Asset(
                data_uri=f"{data_uri}/partition-{i}.parquet",
                is_directory=False,
            )
            for i in range(structure.npartitions)
        ]
        return assets

    def write_partition(self, data, partition):
        uri = self.partition_paths[partition]
        data.to_parquet(uri)

    def write(self, data):
        if self.structure().npartitions != 1:
            raise NotImplementedError
        uri = self.partition_paths[0]
        data.to_parquet(uri)

    def read(self, *args, **kwargs):
        return self.dataframe_adapter.read(*args, **kwargs)

    def read_partition(self, *args, **kwargs):
        return self.dataframe_adapter.read_partition(*args, **kwargs)

    def structure(self):
        return self._structure
