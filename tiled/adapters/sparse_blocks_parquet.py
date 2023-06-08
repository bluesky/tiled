import itertools
from pathlib import Path

import dask.dataframe

from ..structures.core import StructureFamily
from .dataframe import DataFrameAdapter


class ParquetDatasetAdapter:
    structure_family = StructureFamily.sparse

    def __init__(
        self,
        *partition_paths,
        meta,
        divisions,
        metadata=None,
        specs=None,
        references=None,
    ):
        self.partition_paths = partition_paths
        self.meta = meta
        self.divisions = divisions
        self.metadata = metadata or {}
        self.specs = list(specs or [])
        self.references = list(references or [])

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
    def init_storage(cls, directory, chunks):
        from ..server.schemas import Asset

        abs_directory = Path(directory).absolute()
        abs_directory.mkdir()
        num_blocks = (range(len(n)) for n in chunks)
        assets = []
        for block in itertools.product(*num_blocks):
            filepath = (
                directory.absolute() / f"block-{'.'.join(map(str, block))}.parquet"
            )
            assets.append(
                Asset(
                    data_uri=f"file://localhost{filepath}",
                    is_directory=False,
                )
            )
        return assets

    def write_block(self, data, block):
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
