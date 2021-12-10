import dask.array

from ..server.object_cache import get_object_cache
from ..structures.array import ArrayMacroStructure, BuiltinDtype, StructDtype
from ..utils import DictView


class ArrayAdapter:
    """
    Wrap an array-like object in an interface that Tiled can serve.

    Examples
    --------

    Wrap a dask array.

    >>> ArrayAdapter(dask.array.from_array(numpy.random.random((100, 100)), chunks=(100, 50)))

    Wrap a numpy array. Internally, it will be automatically divided into chunks by dask.

    >>> ArrayAdapter.from_array(numpy.random.random((100, 100)))
    """

    structure_family = "array"

    def __init__(self, data, *, metadata=None, dims=None):
        if not isinstance(data, dask.array.Array):
            raise TypeError(f"data must be a dask.array.Array, not a {type(data)}")
        self._data = data
        self._metadata = metadata or {}
        self._dims = dims

    @classmethod
    def from_array(cls, data, *, metadata=None, dims=None):
        return cls(dask.array.from_array(data), metadata=metadata, dims=dims)

    def __repr__(self):
        return f"{type(self).__name__}({self._data!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        "Structures of the layout of blocks of this array"
        return ArrayMacroStructure(
            shape=self._data.shape, chunks=self._data.chunks, dims=self._dims
        )

    def microstructure(self):
        "Internal structure of a block of this array --- i.e. its data type"
        if self._data.dtype.fields is not None:
            micro = StructDtype.from_numpy_dtype(self._data.dtype)
        else:
            micro = BuiltinDtype.from_numpy_dtype(self._data.dtype)
        return micro

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
