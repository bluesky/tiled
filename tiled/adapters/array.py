import dask.array
import numpy
from dask.array.core import normalize_chunks

from ..server.object_cache import get_object_cache
from ..structures.array import ArrayMacroStructure, BuiltinDtype, StructDtype
from ..structures.core import StructureFamily


class ArrayAdapter:
    """
    Wrap an array-like object in an interface that Tiled can serve.

    Examples
    --------

    Wrap any array-like.

    >>> ArrayAdapter(numpy.random.random((100, 100)))

    >>> ArrayAdapter(dask.array.from_array(numpy.random.random((100, 100)), chunks=(100, 50)))

    """

    structure_family = StructureFamily.array

    def __init__(
        self,
        array,
        *,
        shape=None,
        chunks=None,
        metadata=None,
        dims=None,
        specs=None,
        access_policy=None,
    ):
        # Why would shape ever be different from array.shape, you ask?
        # Some formats (notably Zarr) force shape to be a multiple of
        # a chunk size, such that array.shape may include a margin beyond the
        # actual data.
        if shape is None:
            shape = array.shape
        self._shape = shape
        if chunks is None:
            if hasattr(array, "chunks"):
                chunks = array.chunks  # might be None
            else:
                chunks = None
            if chunks is None:
                chunks = ("auto",) * len(shape)
        self._chunks = normalize_chunks(
            chunks,
            shape=shape,
            dtype=array.dtype,
        )
        self._array = array
        self._metadata = metadata or {}
        self._dims = dims
        self.specs = specs or []

    @classmethod
    def from_array(
        cls,
        array,
        *,
        chunks=None,
        metadata=None,
        dims=None,
        specs=None,
        access_policy=None,
    ):
        return cls(
            numpy.asarray(array),
            chunks=chunks,
            metadata=metadata,
            dims=dims,
            specs=specs,
            access_policy=access_policy,
        )

    def __repr__(self):
        return f"{type(self).__name__}({self._array!r})"

    @property
    def dims(self):
        return self._dims

    def metadata(self):
        return self._metadata

    def macrostructure(self):
        "Structures of the layout of blocks of this array"
        return ArrayMacroStructure(
            shape=self._shape, chunks=self._chunks, dims=self._dims
        )

    def microstructure(self):
        "Internal structure of a block of this array --- i.e. its data type"
        if self._array.dtype.fields is not None:
            micro = StructDtype.from_numpy_dtype(self._array.dtype)
        else:
            micro = BuiltinDtype.from_numpy_dtype(self._array.dtype)
        return micro

    def read(self, slice=None):
        array = self._array
        if slice is not None:
            array = array[slice]
        # Special case for dask to cache computed result in object cache.
        if isinstance(self._array, dask.array.Array):
            # Note: If the cache is set to NO_CACHE, this is a null context.
            with get_object_cache().dask_context:
                return array.compute()
        return array

    def read_block(self, block, slice=None):
        # Slice the whole array to get this block.
        slice_, _ = slice_and_shape_from_block_and_chunks(block, self._chunks)
        array = self._array[slice_]
        # Slice within the block.
        if slice is not None:
            array = array[slice]
        # Special case for dask to cache computed result in object cache.
        if isinstance(array, dask.array.Array):
            # Note: If the cache is set to NO_CACHE, this is a null context.
            with get_object_cache().dask_context:
                return array.compute()
        return array


def slice_and_shape_from_block_and_chunks(block, chunks):
    """
    Given dask-like chunks and block id, return slice and shape of the block.
    """
    slice_ = []
    shape = []
    for b, c in zip(block, chunks):
        start = sum(c[:b])
        dim = c[b]
        slice_.append(slice(start, start + dim))
        shape.append(dim)
    return tuple(slice_), tuple(shape)
