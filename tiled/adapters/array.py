from typing import Any, List, Optional, Self, Tuple, Union

import dask.array
from numpy import dtype, ndarray
from numpy.typing import NDArray
from type_alliases import JSON, Spec

from ..access_policies import DummyAccessPolicy, SimpleAccessPolicy
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
        array: NDArray[Any],
        structure: ArrayStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[Union[SimpleAccessPolicy, DummyAccessPolicy]] = None,
    ) -> None:
        self._array = array
        self._structure = structure
        self._metadata = metadata or {}
        self.specs = specs or []

    @classmethod
    def from_array(
        cls,
        array: NDArray[Any],
        *,
        shape: Optional[Tuple[int, ...]] = None,
        chunks: Optional[Tuple[Tuple[int, ...], ...]] = None,
        dims: Optional[Tuple[str, ...]] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[Union[SimpleAccessPolicy, DummyAccessPolicy]] = None,
    ) -> Self:
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

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._array!r})"

    @property
    def dims(self) -> Optional[Tuple[str, ...]]:
        return self._structure.dims

    def metadata(self) -> JSON:
        return self._metadata

    def structure(self) -> ArrayStructure:
        return self._structure

    def read(self, slice: Optional[slice]) -> ndarray[Any, dtype[Any]]:
        array = self._array
        if slice is not None:
            array = array[slice]
        if isinstance(self._array, dask.array.Array):
            return array.compute()
        return array

    def read_block(
        self,
        block: Tuple[int, ...],
        slice: slice,
    ) -> ndarray[Any, dtype[Any]]:
        # Slice the whole array to get this block.
        slice_, _ = slice_and_shape_from_block_and_chunks(block, self._structure.chunks)
        array = self._array[slice_]
        # Slice within the block.
        if slice is not None:
            array = array[slice]
        if isinstance(self._array, dask.array.Array):
            return array.compute()
        return array


def slice_and_shape_from_block_and_chunks(
    block: Tuple[int, ...], chunks: Tuple[Tuple[int, ...], ...]
) -> Tuple[Tuple[slice, ...], Tuple[int, ...]]:
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
