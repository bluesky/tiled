import sys
from pathlib import Path

import numpy
import zarr.storage

from tiled.adapters.array import ArrayAdapter, slice_and_shape_from_block_and_chunks


class WritingArrayAdapter(ArrayAdapter):
    @classmethod
    def new(cls, context, node):
        data_source = node.data_sources[0]
        # Zarr requires evently-sized chunks within each dimension.
        # Use the first chunk along each dimension.
        chunks = tuple(dim[0] for dim in data_source.structure.macro.chunks)
        shape = tuple(dim[0] * len(dim) for dim in data_source.structure.macro.chunks)
        storage = zarr.storage.DirectoryStore(str(safe_path(data_source.data_url.path)))
        zarr.storage.init_array(
            storage,
            shape=shape,
            chunks=chunks,
            dtype=data_source.structure.micro.to_numpy_dtype(),
        )
        return cls.from_node(context, node)

    @classmethod
    def from_node(cls, context, node):
        data_source = node.data_sources[0]
        array = zarr.open_array(str(safe_path(data_source.data_url.path)), "r+")
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


def safe_path(path):
    if sys.platform == "win32" and path[0] == "/":
        path = path[1:]
    return Path(path)
