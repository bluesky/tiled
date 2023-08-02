import dask.array

from ..server.object_cache import get_object_cache
from ..structures.array import ArrayStructure
from ..structures.core import StructureFamily


class ArrayAdapter:
    """
    Wrap an array-like object in an interface that Tiled can serve.

    Examples
    --------

    Wrap any array-like.

    >>> ArrayAdapter.from_array(numpy.random.random((100, 100)))

    >>> ArrayAdapter.from_array(dask.array.from_array(numpy.random.random((100, 100)), chunks=(100, 50)))

    """

    structure_family = StructureFamily.array

    def __init__(
        self,
        array,
        structure,
        *,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        self._array = array
        self._structure = structure
        self._metadata = metadata or {}
        self.specs = specs or []

    @classmethod
    def from_array(
        cls,
        array,
        *,
        shape=None,
        chunks=None,
        dims=None,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        structure = ArrayStructure.from_array(
            array, shape=shape, chunks=chunks, dims=dims
        )
        return cls(
            array,
            structure=structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

    def __repr__(self):
        return f"{type(self).__name__}({self._array!r})"

    @property
    def dims(self):
        return self._structure.dims

    def metadata(self):
        return self._metadata

    def structure(self):
        return self._structure

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
        slice_, _ = slice_and_shape_from_block_and_chunks(block, self._structure.chunks)
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
