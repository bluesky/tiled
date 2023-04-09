import builtins

import zarr.storage

from tiled.adapters.array import ArrayAdapter, slice_and_shape_from_block_and_chunks


class ZarrAdapter(ArrayAdapter):
    @classmethod
    def new(cls, directory, dtype, shape, chunks):
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
        return cls.from_directory(directory, shape=shape, chunks=chunks)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # This is used to trim overflow because Zarr always has equal-sized
        # chunks.
        self.__stencil = tuple(
            builtins.slice(0, dim) for dim in self.macrostructure().shape
        )

    @classmethod
    def from_directory(cls, directory, shape=None, chunks=None):
        array = zarr.open_array(str(directory), "r+")
        return cls(array, shape=shape, chunks=chunks)

    def read(self, slice=...):
        # Trim overflow because Zarr always has equal-sized chunks.
        return self._array[self.__stencil][slice]

    def read_block(self, block, slice=...):
        block_slice, _ = slice_and_shape_from_block_and_chunks(
            block, self.macrostructure().chunks
        )
        # Slice the block out of the whole array,
        # and optionally a sub-slice therein.
        return self._array[self.__stencil][block_slice][slice]

    def write(self, data, slice=...):
        self._array[self.__stencil][slice] = data

    async def write_block(self, block, data, slice=...):
        block_slice, shape = slice_and_shape_from_block_and_chunks(
            block, self.macrostructure().chunks
        )
        self._array[self.__stencil][block_slice][slice] = data
