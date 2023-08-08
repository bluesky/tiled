import itertools
from urllib import parse

import numpy
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


class SparseBlocksParquetAdapter:
    structure_family = StructureFamily.sparse

    def __init__(
        self,
        *block_uris,
        structure,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        num_blocks = (range(len(n)) for n in structure.chunks)
        self.blocks = {}
        for block, uri in zip(itertools.product(*num_blocks), sorted(block_uris)):
            self.blocks[block] = uri
        self._structure = structure
        self._metadata = metadata or {}
        self.specs = list(specs or [])
        self.access_policy = access_policy

    @classmethod
    def init_storage(
        cls,
        directory,
        structure,
    ):
        from ..server.schemas import Asset

        directory.mkdir()

        num_blocks = (range(len(n)) for n in structure.chunks)
        block_uris = []
        for block in itertools.product(*num_blocks):
            filepath = directory / f"block-{'.'.join(map(str, block))}.parquet"
            uri = parse.urlunparse(("file", "localhost", str(filepath), "", "", None))
            block_uris.append(uri)
        assets = [
            Asset(
                data_uri=uri,
                is_directory=False,
            )
            for uri in block_uris
        ]
        return assets

    def metadata(self):
        return self._metadata

    def write_block(self, data, block):
        uri = self.blocks[block]
        data.to_parquet(uri)

    def write(self, data):
        if len(self.blocks) > 1:
            raise NotImplementedError
        uri = self.blocks[(0,) * len(self._structure.shape)]
        data.to_parquet(uri)

    def read(self, slice=...):
        all_coords = []
        all_data = []
        for block, uri in self.blocks.items():
            coords, data = load_block(uri)
            offsets = []
            for b, c in zip(block, self.structure().chunks):
                offset = sum(c[:b])
                offsets.append(offset)
            global_coords = coords + [[i] for i in offsets]
            all_coords.append(global_coords)
            all_data.append(data)
        arr = sparse.COO(
            data=numpy.concatenate(all_data),
            coords=numpy.concatenate(all_coords, axis=-1),
            shape=self._structure.shape,
        )
        return arr[slice]

    def read_block(self, block, slice=...):
        coords, data = load_block(self.blocks[block])
        _, shape = slice_and_shape_from_block_and_chunks(block, self._structure.chunks)
        arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
        return arr[slice]

    def structure(self):
        return self._structure
