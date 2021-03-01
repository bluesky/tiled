import dask.array

from ..containers.array import ArrayStructure, MachineDataType


class ArraySource:
    """
    Wrap an array-like

    Such as:

    - numpy.ndarray
    - dask.array.Array
    - h5py.Dataset
    """

    container = "array"

    def __init__(self, data):
        self.metadata = {}
        if not isinstance(data, dask.array.Array):
            data = dask.array.from_array(data)
        self._data = data

    def __repr__(self):
        return f"{type(self).__name__}({self._data!r})"

    def describe(self):
        return ArrayStructure(
            shape=self._data.shape,
            chunks=self._data.chunks,
            dtype=MachineDataType.from_numpy_dtype(self._data.dtype),
        )

    def read(self):
        return self._data
