import contextlib
from typing import Any, List, Optional, Set, Tuple

import dask.array
import numpy
import pandas
from numpy.typing import NDArray

from ..ndslice import NDSlice
from ..storage import Storage
from ..structures.array import ArrayStructure
from ..structures.core import Spec, StructureFamily
from ..type_aliases import JSON


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
    supported_storage: Set[type[Storage]] = set()

    def __init__(
        self,
        array: NDArray[Any],
        structure: ArrayStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        """

        Parameters
        ----------
        array :
        structure :
        metadata :
        specs :
        """
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
    ) -> "ArrayAdapter":
        """

        Parameters
        ----------
        array :
        shape :
        chunks :
        dims :
        metadata :
        specs :

        Returns
        -------

        """
        # May be a list of something; convert to array
        if not hasattr(array, "__array__"):
            array = numpy.asanyarray(array)

        # Convert array of arrays to ND array to expose the underlying dtype
        is_array_of_arrays = (
            array.dtype == "object"
            and array.shape[0]
            and isinstance(array[0], numpy.ndarray)
        )
        if is_array_of_arrays:
            with contextlib.suppress(ValueError):
                # only uniform arrays (with same dimensions) are stackable
                array = numpy.vstack(array)

        # Convert (experimental) pandas.StringDtype to numpy's unicode string dtype
        is_likely_string_dtype = isinstance(array.dtype, pandas.StringDtype) or (
            array.dtype == "object" and array.dtype.fields is None
        )
        if is_likely_string_dtype:
            array = numpy.array([str(x) for x in array])  # becomes "<Un" dtype

        structure = ArrayStructure.from_array(
            array, shape=shape, chunks=chunks, dims=dims
        )
        return cls(
            array,
            structure=structure,
            metadata=metadata,
            specs=specs,
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

    def read(
        self,
        slice: NDSlice = NDSlice(...),
    ) -> NDArray[Any]:
        """

        Parameters
        ----------
        slice :

        Returns
        -------

        """
        # _array[...] requires an actual tuple, not just a subclass of tuple
        array = self._array[tuple(slice)] if slice else self._array
        if isinstance(self._array, dask.array.Array):
            return array.compute()
        return array

    def read_block(
        self,
        block: Tuple[int, ...],
        slice: NDSlice = NDSlice(...),
    ) -> NDArray[Any]:
        """

        Parameters
        ----------
        block :
        slice :

        Returns
        -------

        """
        # Slice the whole array to get this block.
        slice_, _ = slice_and_shape_from_block_and_chunks(block, self._structure.chunks)
        # _array[...] requires an actual tuple, not just a subclass of tuple
        array = self._array[tuple(slice_)]
        # Slice within the block.
        array = array[slice] if slice else array
        if isinstance(self._array, dask.array.Array):
            return array.compute()
        return array


def slice_and_shape_from_block_and_chunks(
    block: Tuple[int, ...], chunks: Tuple[Tuple[int, ...], ...]
) -> tuple[NDSlice, NDSlice]:
    """
    Given dask-like chunks and block id, return slice and shape of the block.
    Parameters
    ----------
    block :
    chunks :

    Returns
    -------

    """
    slice_ = []
    shape = []
    for b, c in zip(block, chunks):
        start = sum(c[:b])
        dim = c[b]
        slice_.append(slice(start, start + dim))
        shape.append(dim)
    return NDSlice(*slice_), NDSlice(*shape)
