import dask.array

from ..structures.array import ArrayMacroStructure, MachineDataType
from ..structures.structured_array import ArrayTabularMacroStructure, StructDtype
from ..utils import DictView
from ..server.object_cache import get_object_cache


class ArrayAdapter:
    """
    Wrap an array-like in a "Reader".

    Examples
    --------

    Wrap a dask array.

    >>> ArrayAdapter(dask.array.from_array(numpy.random.random((100, 100)), chunks=(100, 50)))

    Wrap a numpy array. Internally, it will be automatically divided into chunks by dask.

    >>> ArrayAdapter.from_array(numpy.random.random((100, 100)))
    """

    structure_family = "array"

    def __init__(self, data, metadata=None):
        if not isinstance(data, dask.array.Array):
            raise TypeError(f"data must be a dask.array.Array, not a {type(data)}")
        self._data = data
        self._metadata = metadata or {}

    @classmethod
    def from_array(cls, data, metadata=None):
        return cls(dask.array.from_array(data), metadata)

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

    def read(self, slice=None):
        dask_array = self._data
        if slice is not None:
            dask_array = dask_array[slice]
        # Note: If the cache is set to NO_CACHE, this is a null context.
        with get_object_cache().dask_context:
            return dask_array.compute()

    def read_block(self, block, slice=None):
        dask_array = self._data.blocks[block]
        if slice is not None:
            dask_array = dask_array[slice]
        # Note: If the cache is set to NO_CACHE, this is a null context.
        with get_object_cache().dask_context:
            return dask_array.compute()


class StructuredArrayGenericAdapter(ArrayAdapter):
    structure_family = "structured_array_generic"

    def macrostructure(self):
        "Structures of the layout of blocks of this array"
        return ArrayMacroStructure(
            shape=self._data.shape,
            chunks=self._data.chunks,
        )

    def microstructure(self):
        "Internal structure of a block of this array --- i.e. its data type"
        return StructDtype.from_numpy_dtype(self._data.dtype)


class StructuredArrayTabularAdapter(ArrayAdapter):
    structure_family = "structured_array_tabular"

    def macrostructure(self):
        "Structures of the layout of blocks of this array"
        return ArrayTabularMacroStructure(
            shape=self._data.shape,
            chunks=self._data.chunks,
        )

    def microstructure(self):
        "Internal structure of a block of this array --- i.e. its data type"
        return StructDtype.from_numpy_dtype(self._data.dtype)
