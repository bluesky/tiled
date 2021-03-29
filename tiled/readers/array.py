import dask.array

from ..structures.array import ArrayMacroStructure, MachineDataType
from ..utils import DictView


class ArrayReader:
    """
    Wrap an array-like

    Such as:

    - numpy.ndarray
    - dask.array.Array
    - h5py.Dataset
    """

    structure_family = "array"

    def __init__(self, data, metadata=None):
        self._metadata = metadata or {}
        if not isinstance(data, dask.array.Array):
            data = dask.array.from_array(data)
        self._data = data

    def __repr__(self):
        return f"{type(self).__name__}({self._data!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        "Structures of the layout of blocks of this array"
        return ArrayMacroStructure(
            shape=self._data.shape,
            chunks=self._data.chunks,
        )

    def microstructure(self):
        "Internal structure of a block of this array --- i.e. its data type"
        return MachineDataType.from_numpy_dtype(self._data.dtype)

    def read(self):
        return self._data

    def read_block(self, block, slice=None):
        dask_array = self._data.blocks[block]
        if slice is not None:
            dask_array = dask_array[slice]
        return dask_array.compute()

    def close(self):
        # Allow the garbage collector to reclaim this memory.
        self._data = None

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()
