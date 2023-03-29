import numpy

from ..adapters.array import ArrayAdapter, slice_and_shape_from_block_and_chunks
from .node import BaseAdapter


class ArrayAdapter(BaseAdapter):
    def put_data(self, body, block=None):
        macrostructure = self.macrostructure()
        if block is None:
            shape = macrostructure.shape
            slice_ = numpy.s_[:]
        else:
            slice_, shape = slice_and_shape_from_block_and_chunks(
                block, macrostructure.chunks
            )
        array = numpy.frombuffer(
            body, dtype=self.microstructure().to_numpy_dtype()
        ).reshape(shape)
        self._array[slice_] = array
