import builtins
import collections.abc
import os
import sys
from typing import Any, Iterator, List, Optional, Tuple, Union

import zarr.core
import zarr.hierarchy
import zarr.storage
from numpy._typing import NDArray

from ..adapters.utils import IndexersMixin
from ..iterviews import ItemsView, KeysView, ValuesView
from ..server.schemas import Asset
from ..structures.array import ArrayStructure
from ..structures.core import Spec, StructureFamily
from ..type_aliases import JSON, NDSlice
from ..utils import Conflicts, node_repr, path_from_uri
from .array import ArrayAdapter, slice_and_shape_from_block_and_chunks
from .protocols import AccessPolicy

INLINED_DEPTH = int(os.getenv("TILED_HDF5_INLINED_CONTENTS_MAX_DEPTH", "7"))


def read_zarr(
    data_uri: str,
    structure: Optional[ArrayStructure] = None,
    **kwargs: Any,
) -> Union["ZarrGroupAdapter", ArrayAdapter]:
    """

    Parameters
    ----------
    data_uri :
    structure :
    kwargs :

    Returns
    -------

    """
    filepath = path_from_uri(data_uri)
    zarr_obj = zarr.open(filepath)  # Group or Array
    adapter: Union[ZarrGroupAdapter, ArrayAdapter]
    if isinstance(zarr_obj, zarr.hierarchy.Group):
        adapter = ZarrGroupAdapter(zarr_obj, **kwargs)
    else:
        if structure is None:
            adapter = ZarrArrayAdapter.from_array(zarr_obj, **kwargs)
        else:
            adapter = ZarrArrayAdapter(zarr_obj, structure=structure, **kwargs)
    return adapter


class ZarrArrayAdapter(ArrayAdapter):
    """ """

    @classmethod
    def init_storage(cls, data_uri: str, structure: ArrayStructure) -> List[Asset]:
        """

        Parameters
        ----------
        data_uri :
        structure :

        Returns
        -------

        """
        # Zarr requires evenly-sized chunks within each dimension.
        # Use the first chunk along each dimension.
        zarr_chunks = tuple(dim[0] for dim in structure.chunks)
        shape = tuple(dim[0] * len(dim) for dim in structure.chunks)
        directory = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        storage = zarr.storage.DirectoryStore(str(directory))
        zarr.storage.init_array(
            storage,
            shape=shape,
            chunks=zarr_chunks,
            dtype=structure.data_type.to_numpy_dtype(),
        )
        return [
            Asset(
                data_uri=data_uri,
                is_directory=True,
                parameter="data_uri",
            )
        ]

    def _stencil(self) -> Tuple[slice, ...]:
        """
        Trims overflow because Zarr always has equal-sized chunks.
        Returns
        -------

        """
        return tuple(builtins.slice(0, dim) for dim in self.structure().shape)

    def read(
        self,
        slice: NDSlice = ...,
    ) -> NDArray[Any]:
        """

        Parameters
        ----------
        slice :

        Returns
        -------

        """
        return self._array[self._stencil()][slice]

    def read_block(
        self,
        block: Tuple[int, ...],
        slice: NDSlice = ...,
    ) -> NDArray[Any]:
        """

        Parameters
        ----------
        block :
        slice :

        Returns
        -------

        """
        block_slice, _ = slice_and_shape_from_block_and_chunks(
            block, self.structure().chunks
        )
        # Slice the block out of the whole array,
        # and optionally a sub-slice therein.
        return self._array[self._stencil()][block_slice][slice]

    def write(
        self,
        data: NDArray[Any],
        slice: NDSlice = ...,
    ) -> None:
        """

        Parameters
        ----------
        data :
        slice :

        Returns
        -------

        """
        if slice is not ...:
            raise NotImplementedError
        self._array[self._stencil()] = data

    async def write_block(
        self,
        data: NDArray[Any],
        block: Tuple[int, ...],
    ) -> None:
        """

        Parameters
        ----------
        data :
        block :
        slice :

        Returns
        -------

        """
        block_slice, shape = slice_and_shape_from_block_and_chunks(
            block, self.structure().chunks
        )
        self._array[block_slice] = data

    async def patch(
        self,
        data: NDArray[Any],
        offset: Tuple[int, ...],
        extend: bool = False,
    ) -> Tuple[Tuple[int, ...], Tuple[Tuple[int, ...], ...]]:
        """
        Write data into a slice of the array, maybe extending it.

        If the specified slice does not fit into the array, and extend=True, the
        array will be resized (expanded, never shrunk) to fit it.

        Parameters
        ----------
        data : array-like
        offset : tuple[int]
            Where to place the new data
        extend : bool
            If slice does not fit wholly within the shape of the existing array,
            reshape (expand) it to fit if this is True.

        Raises
        ------
        ValueError :
            If slice does not fit wholly with the shape of the existing array
            and expand is False
        """
        current_shape = self._array.shape
        normalized_offset = [0] * len(current_shape)
        normalized_offset[: len(offset)] = list(offset)
        new_shape = []
        slice_ = []
        for data_dim, offset_dim, current_dim in zip(
            data.shape, normalized_offset, current_shape
        ):
            new_shape.append(max(current_dim, data_dim + offset_dim))
            slice_.append(slice(offset_dim, offset_dim + data_dim))
        new_shape_tuple = tuple(new_shape)
        if new_shape_tuple != current_shape:
            if extend:
                # Resize the Zarr array to accommodate new data
                self._array.resize(new_shape_tuple)
            else:
                raise Conflicts(
                    f"Slice {slice} does not fit into array shape {current_shape}. "
                    "Use ?extend=true to extend array dimension to fit."
                )
        self._array[tuple(slice_)] = data
        new_chunks = []
        # Zarr has regularly-sized chunks, so no user input is required to
        # simply extend the existing pattern.
        for chunk_size, size in zip(self._array.chunks, new_shape_tuple):
            dim = [chunk_size] * (size // chunk_size)
            if size % chunk_size:
                dim.append(size % chunk_size)
            new_chunks.append(tuple(dim))
        new_chunks_tuple = tuple(new_chunks)
        return new_shape_tuple, new_chunks_tuple


if sys.version_info < (3, 9):
    from typing_extensions import Mapping

    MappingType = Mapping
else:
    import collections

    MappingType = collections.abc.Mapping


class ZarrGroupAdapter(
    MappingType[str, Union["ArrayAdapter", "ZarrGroupAdapter"]],
    IndexersMixin,
):
    """ """

    structure_family = StructureFamily.container

    def __init__(
        self,
        node: Any,
        *,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """

        Parameters
        ----------
        node :
        structure :
        metadata :
        specs :
        access_policy :
        """
        if structure is not None:
            raise ValueError(
                f"structure is expected to be None for containers, not {structure}"
            )
        self._node = node
        self._access_policy = access_policy
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        super().__init__()

    def __repr__(self) -> str:
        """

        Returns
        -------

        """
        return node_repr(self, list(self))

    @property
    def access_policy(self) -> Optional[AccessPolicy]:
        """

        Returns
        -------

        """
        return self._access_policy

    def metadata(self) -> Any:
        """

        Returns
        -------

        """
        return self._node.attrs

    def structure(self) -> None:
        """

        Returns
        -------

        """
        return None

    def __iter__(self) -> Iterator[Any]:
        """

        Returns
        -------

        """
        yield from self._node

    def __getitem__(self, key: str) -> Union[ArrayAdapter, "ZarrGroupAdapter"]:
        """

        Parameters
        ----------
        key :

        Returns
        -------

        """
        value = self._node[key]
        if isinstance(value, zarr.hierarchy.Group):
            return ZarrGroupAdapter(value)
        else:
            return ZarrArrayAdapter.from_array(value)

    def __len__(self) -> int:
        """

        Returns
        -------

        """
        return len(self._node)

    def keys(self) -> KeysView:  # type: ignore
        """

        Returns
        -------

        """
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self) -> ValuesView:  # type: ignore
        """

        Returns
        -------

        """
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self) -> ItemsView:  # type: ignore
        """

        Returns
        -------

        """
        return ItemsView(lambda: len(self), self._items_slice)

    def search(self, query: Any) -> None:
        """

        Parameters
        ----------
        query :

        Returns
        -------
                Return a Tree with a subset of the mapping.

        """
        raise NotImplementedError

    def read(self, fields: Optional[str]) -> "ZarrGroupAdapter":
        """

        Parameters
        ----------
        fields :

        Returns
        -------

        """
        if fields is not None:
            raise NotImplementedError
        return self

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(self, start: int, stop: int, direction: int) -> List[Any]:
        """

        Parameters
        ----------
        start :
        stop :
        direction :

        Returns
        -------

        """
        keys = list(self._node)
        if direction < 0:
            keys = list(reversed(keys))
        return keys[start:stop]

    def _items_slice(self, start: int, stop: int, direction: int) -> List[Any]:
        """

        Parameters
        ----------
        start :
        stop :
        direction :

        Returns
        -------

        """
        items = [(key, self[key]) for key in list(self)]
        if direction < 0:
            items = list(reversed(items))
        return items[start:stop]

    def inlined_contents_enabled(self, depth: int) -> bool:
        """

        Parameters
        ----------
        depth :

        Returns
        -------

        """
        return depth <= INLINED_DEPTH
