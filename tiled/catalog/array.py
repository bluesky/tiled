import numpy
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
        return cls.from_directory(directory)

    @classmethod
    def from_directory(cls, directory):
        array = zarr.open_array(str(directory), "r+")
        return cls.from_array(array)

    def put_data(self, body, block=None):
        # Organize files into subdirectories with the first two
        # characters of the key to avoid one giant directory.
        if block:
            slice_, shape = slice_and_shape_from_block_and_chunks(
                block, self.doc.structure.macro.chunks
            )
        else:
            slice_ = numpy.s_[:]
            shape = self.doc.structure.macro.shape
        array = numpy.frombuffer(
            body, dtype=self.doc.structure.micro.to_numpy_dtype()
        ).reshape(shape)
        self.array[slice_] = array
