import builtins
from pathlib import Path

import zarr.storage

from .array import ArrayAdapter, slice_and_shape_from_block_and_chunks


class ZarrAdapter(ArrayAdapter):
    @classmethod
    def init_storage(cls, directory, dtype, shape, chunks):
        from ..server.schemas import Asset

        # Zarr requires evently-sized chunks within each dimension.
        # Use the first chunk along each dimension.
        zarr_chunks = tuple(dim[0] for dim in chunks)
        shape = tuple(dim[0] * len(dim) for dim in chunks)
        storage = zarr.storage.DirectoryStore(str(directory))
        zarr.storage.init_array(
            storage,
            shape=shape,
            chunks=zarr_chunks,
            dtype=dtype,
        )
        return [Asset(data_uri=f"file://localhost{Path(directory).absolute()}", is_directory=True)]

    @classmethod
    def from_directory(cls, directory, *, shape=None, chunks=None, metadata=None, specs=None, references=None):
        array = zarr.open_array(str(directory), "r+")
        return cls(array, shape=shape, chunks=chunks, metadata=metadata, specs=specs, references=references)

    def _stencil(self):
        "Trims overflow because Zarr always has equal-sized chunks."
        return tuple(builtins.slice(0, dim) for dim in self.macrostructure().shape)

    def read(self, slice=...):
        return self._array[self._stencil()][slice]

    def read_block(self, block, slice=...):
        block_slice, _ = slice_and_shape_from_block_and_chunks(
            block, self.macrostructure().chunks
        )
        # Slice the block out of the whole array,
        # and optionally a sub-slice therein.
        return self._array[self._stencil()][block_slice][slice]

    def write(self, data, slice=...):
        if slice is not ...:
            raise NotImplementedError
        self._array[self._stencil()] = data

    async def write_block(self, data, block, slice=...):
        if slice is not ...:
            raise NotImplementedError
        block_slice, shape = slice_and_shape_from_block_and_chunks(
            block, self.macrostructure().chunks
        )
        self._array[block_slice] = data
