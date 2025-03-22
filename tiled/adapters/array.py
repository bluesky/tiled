import dataclasses
import urllib.parse
from typing import Annotated, Any, List, Literal, Optional, Tuple, Union

import dask.array
import numpy
import pandas
from dask.array.core import normalize_chunks
from fastapi import Query
from ndindex import ndindex
from numpy.typing import NDArray

from ..structures.array import ArrayStructure, BuiltinDtype, StructDtype
from ..structures.core import Spec, StructureFamily
from ..type_aliases import JSON, NDSlice

DIM_REGEX = r"(?:(?:-?\d+)?:){0,2}(?:-?\d+)?"
SLICE_REGEX = rf"^{DIM_REGEX}(?:,{DIM_REGEX})*$"
RESHAPE_REGEX = r"^((?:-1)|\d+)(?:,((?:-1)|\d+))*$"  # NOTE: not strict (-1, 0)
RECHUNK_REGEX = r"^((?:auto)|\d+)(?:,((?:auto)|\d+))*$"

MAX_CHUNK_SIZE = 100_000_000  # ~100 MB


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
        # Convert (experimental) pandas.StringDtype to numpy's unicode string dtype
        if isinstance(array.dtype, pandas.StringDtype):
            max_size = max((len(i) for i in array.ravel()))
            array = array.astype(dtype=numpy.dtype(f"<U{max_size}"))

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
        """

        Returns
        -------

        """
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
        slice: Optional[NDSlice] = None,
    ) -> NDArray[Any]:
        """

        Parameters
        ----------
        slice :

        Returns
        -------

        """
        array = self._array[tuple(slice)] if slice else self._array
        if isinstance(self._array, dask.array.Array):
            return array.compute()
        return array

    def read_block(
        self,
        block: Tuple[int, ...],
        slice: Optional[NDSlice] = None,
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
        # _array[...] requires a tuple, not just a subclass of tuple.
        array = self._array[tuple(slice_)]
        # Slice within the block.
        if slice is not None:
            array = array[slice]
        if isinstance(self._array, dask.array.Array):
            return array.compute()
        return array


def slice_and_shape_from_block_and_chunks(
    block: Tuple[int, ...], chunks: Tuple[Tuple[int, ...], ...]
) -> tuple[NDSlice, tuple[int, ...]]:
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
    return NDSlice(*slice_), tuple(shape)


@dataclasses.dataclass
class ArrayTransforms:
    """Specify and parse array transformation parameters

    These transformations are to be applied to an array before serving it to
    the client. They are intended to be used for operations that are not too
    expensive to perform on the server side, but that would be inconvenient
    for the client to perform on the raw data.

    Parameters
    ----------
    reslice : str
        A numpy-style slice string to apply to the array.
    rechunk : str
        A chunk size specification to apply to the array.
    reshape : str
        A shape specification to apply to the array.
    asdtype : str
        A dtype specification to apply to the array.
    todense : bool
        Whether to convert a sparse array to its dense representation.
    """

    reslice: Optional[NDSlice] = None
    rechunk: Optional[
        Union[Literal["auto"], tuple[Union[Literal["auto"], int], ...]]
    ] = None
    reshape: Optional[tuple[int, ...]] = None
    asdtype: Optional[numpy.dtype] = None
    todense: Optional[bool] = None

    @classmethod
    def from_query(
        cls,
        reslice: Annotated[Optional[str], Query(pattern=SLICE_REGEX)] = None,
        rechunk: Annotated[Optional[str], Query(pattern=RECHUNK_REGEX)] = None,
        reshape: Annotated[Optional[str], Query(pattern=RESHAPE_REGEX)] = None,
        asdtype: Annotated[Optional[str], Query()] = None,
        todense: Annotated[Optional[bool], Query()] = None,
    ) -> "ArrayTransforms":
        if reslice:
            reslice = NDSlice.from_numpy_str(reslice)
        if reshape:
            reshape = tuple(map(int, reshape.strip("[]()").split(",")))  # type: ignore
        if asdtype:
            asdtype = numpy.dtype(asdtype)
        if reslice or reshape:
            # Always need to rechunk if reslicing or reshaping
            rechunk = rechunk or "auto"

        return cls(
            reslice=reslice,  # type: ignore
            rechunk=rechunk,  # type: ignore
            reshape=reshape,  # type: ignore
            asdtype=asdtype,
            todense=todense,
        )

    def __bool__(self) -> bool:
        return any((self.reslice, self.rechunk, self.reshape, self.asdtype))

    def update_structure(self, structure: ArrayStructure) -> ArrayStructure:
        if self.reslice:
            structure.shape = ndindex(self.reslice).newshape(structure.shape)

        if self.reshape:
            # TODO: Determine shape
            structure.shape = numpy.empty(structure.shape).reshape(self.reshape).shape

        if self.asdtype:
            if isinstance(structure.data_type, StructDtype):
                raise ValueError("Cannot change dtype of a structured array")
            structure.data_type = BuiltinDtype.from_numpy_dtype(self.asdtype)

        if self.rechunk:
            structure.chunks = normalize_chunks(
                self.rechunk,
                structure.shape,
                limit=MAX_CHUNK_SIZE,
                dtype=structure.data_type.to_numpy_dtype(),
            )

        return structure

    def update_links(self, links: dict[str, str]) -> dict[str, str]:
        for key, link in links.items():
            parsed = urllib.parse.urlparse(link)
            query_dict = urllib.parse.parse_qs(parsed.query)
            if self.reslice:
                query_dict["reslice"] = [self.reslice.to_numpy_str()]
            if self.reshape:
                query_dict["reshape"] = [",".join(map(str, self.reshape))]
            if self.asdtype:
                query_dict["asdtype"] = [self.asdtype.name]
            if self.rechunk:
                if self.rechunk != "auto" or not (self.reslice or self.reshape):
                    # When reslicing or reshaping, auto rechunking is implicit
                    query_dict["rechunk"] = [str(self.rechunk)]

            query_str = urllib.parse.urlencode(query_dict, doseq=True)
            query_str = urllib.parse.unquote(query_str)  # Make it human-readable

            links[key] = urllib.parse.urlunparse(parsed._replace(query=query_str))

        return links

    def apply(self, arr: NDArray[Any]) -> NDArray[Any]:
        if self.reslice:
            arr = arr[self.reslice]

        if self.reshape:
            arr = arr.reshape(self.reshape)

        if self.asdtype:
            arr = arr.astype(self.asdtype)

        return arr
