import itertools
from pathlib import Path
from urllib import parse

import numpy
import pandas
import sparse

from ..adapters.array import slice_and_shape_from_block_and_chunks
from ..structures.core import StructureFamily
from ..structures.sparse import COOStructure


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
        metadata=None,
        shape=None,
        chunks=None,
        dims=None,
        specs=None,
    ):
        num_blocks = (range(len(n)) for n in chunks)
        self.blocks = {}
        for block, uri in zip(itertools.product(*num_blocks), sorted(block_uris)):
            self.blocks[block] = uri
        self.dims = dims
        self.shape = shape
        self.chunks = chunks
        self.metadata = metadata or {}
        self.specs = list(specs or [])

    @classmethod
    def init_storage(
        cls,
        directory,
        chunks,
    ):
        from ..server.schemas import Asset

        abs_directory = Path(directory).absolute()
        abs_directory.mkdir()
        abs_directory_parse = parse.urlunparse(
            ("file", "localhost", str(Path(directory).absolute()), "", "", None)
        )

        num_blocks = (range(len(n)) for n in chunks)
        block_uris = []
        for block in itertools.product(*num_blocks):
            filepath = (
                abs_directory_parse / f"block-{'.'.join(map(str, block))}.parquet"
            )
            uri = f"file://localhost{filepath}"
            block_uris.append(uri)
        assets = [
            Asset(
                data_uri=uri,
                is_directory=False,
            )
            for uri in block_uris
        ]
        return assets

    def write_block(self, data, block):
        uri = self.blocks[block]
        data.to_parquet(uri)

    def write(self, data):
        if len(self.blocks) > 1:
            raise NotImplementedError
        uri = self.blocks[(0, 0)]
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
            shape=self.shape,
        )
        return arr[slice]

    def read_block(self, block, slice=...):
        coords, data = load_block(self.blocks[block])
        _, shape = slice_and_shape_from_block_and_chunks(block, self.chunks)
        arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
        return arr[slice]

    def structure(self):
        # Convert pydantic implementation to dataclass implemenetation
        # expected by server.
        return COOStructure(
            shape=self.shape,
            chunks=self.chunks,
            dims=self.dims,
        )
