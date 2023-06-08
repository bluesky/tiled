import itertools
from pathlib import Path

import pandas
import sparse

from ..adapters.array import slice_and_shape_from_block_and_chunks
from ..structures.core import StructureFamily


def load_block(uri):
    # TODO This can be done without pandas.
    # Better to use a plain I/O library.
    df = pandas.read_parquet(uri)
    coords = df[df.columns[:-1]].values.T
    data = df["data"].values
    return coords, data


class SparseParquetBlocksAdapter:
    structure_family = StructureFamily.sparse

    def __init__(
        self,
        *block_uris,
        meta,
        divisions,
        metadata=None,
        specs=None,
        references=None,
    ):
        self.block_uris = block_uris
        self.meta = meta
        self.divisions = divisions
        self.metadata = metadata or {}
        self.specs = list(specs or [])
        self.references = list(references or [])

    @classmethod
    def init_storage(cls, directory, chunks):
        from ..server.schemas import Asset

        abs_directory = Path(directory).absolute()
        abs_directory.mkdir()
        num_blocks = (range(len(n)) for n in chunks)
        blocks = {}
        for block in itertools.product(*num_blocks):
            filepath = (
                directory.absolute() / f"block-{'.'.join(map(str, block))}.parquet"
            )
            uri = f"file://localhost{filepath}"
            blocks[block] = uri
        assets = [
            Asset(
                data_uri=uri,
                is_directory=False,
            )
            for uri in blocks.values()
        ]
        return assets

    def write_block(self, data, block):
        uri = self.block_uris[block]
        data.to_parquet(uri)

    def write(self, data):
        if self.macrostructure().npartitions != 1:
            raise NotImplementedError
        uri = self.partition_paths[0]
        data.to_parquet(uri)

    def read(self, *args, **kwargs):
        return self.dataframe_adapter.read(*args, **kwargs)

    def read_block(self, block, slice=...):
        coords, data = load_block(self.block_uris[block])
        _, shape = slice_and_shape_from_block_and_chunks(
            block, self.doc.structure.chunks
        )
        arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
        arr = arr[...]
        return arr

    def macrostructure(self):
        return self.dataframe_adapter.macrostructure()

    def microstructure(self):
        return self.dataframe_adapter.microstructure()
