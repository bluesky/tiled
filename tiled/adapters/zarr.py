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
from ..utils import node_repr, path_from_uri
from .array import ArrayAdapter, slice_and_shape_from_block_and_chunks
from .protocols import AccessPolicy
from .type_alliases import JSON, NDSlice

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
        axis=0,
    ) -> None:
        """
        Appends new_data to the zarr_array along a specified axis, resizing the Zarr array appropriately.

        Parameters:
            zarr_array (zarr.Array): The target Zarr array to append to.
            new_data (np.ndarray): The new data to append, must match the shape of zarr_array along all axes except the specified axis.
            axis (int): The axis along which to append the new data. Default is 0 (first axis).

        Raises:
            ValueError: If new_data shape is incompatible with zarr_array along non-appended axes,
                        or if the specified axis is not chunked (chunk size is 1).
        """

        if slice is not ...:
            raise NotImplementedError
        block_slice, shape = slice_and_shape_from_block_and_chunks(
            block, self.structure().chunks
        )
   
        
        # Ensure the axis is within bounds
        if axis < 0 or axis >= zarr_array.ndim:
            raise ValueError(f"Axis {axis} is out of bounds for zarr_array with {zarr_array.ndim} dimensions.")
        
        # Check if the axis to append along has a chunk size > 1
        if zarr_array.chunks[axis] == 1:
            raise ValueError(f"Appending along axis {axis} may be inefficient because it is not chunked (chunk size is 1). "
                            "Consider appending along an axis with a larger chunk size.")
        
        # Check if new_data has compatible shape with zarr_array along all axes except the specified axis
        for ax in range(zarr_array.ndim):
            if ax != axis and zarr_array.shape[ax] != new_data.shape[ax]:
                raise ValueError(f"Shape mismatch! new_data shape {new_data.shape} is incompatible with zarr_array shape {zarr_array.shape} along axis {ax}.")
        
        # Calculate the new size along the specified axis
        new_size = list(zarr_array.shape)
        new_size[axis] += new_data.shape[axis]
        
        # Resize the Zarr array along the specified axis
        zarr_array.resize(tuple(new_size))
        
        # Define the slice to target the newly added space along the specified axis
        slices = [slice(None)] * zarr_array.ndim  # Create a list of slices for each dimension
        slices[axis] = slice(zarr_array.shape[axis] - new_data.shape[axis], zarr_array.shape[axis])  # Set the slice for the specified axis
        
        # Append new data at the target location in the Zarr array
        zarr_array[tuple(slices)] = new_data

    async def append_block(
        self,
        data: NDArray[Any],
        block: Tuple[int, ...],
        slice: Optional[NDSlice] = ...,
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
        if slice is not ...:
            raise NotImplementedError
        block_slice, shape = slice_and_shape_from_block_and_chunks(
            block, self.structure().chunks
        )

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
