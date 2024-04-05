import builtins
import collections.abc
import os
from types import EllipsisType
from typing import Any, Iterator, Optional, Tuple, Union

import dask
import pandas
import zarr.core
import zarr.hierarchy
import zarr.storage
from numpy._typing import NDArray
from type_alliases import JSON, Spec

from ..access_policies import DummyAccessPolicy, SimpleAccessPolicy
from ..adapters.utils import IndexersMixin
from ..iterviews import ItemsView, KeysView, ValuesView
from ..server.schemas import NodeStructure
from ..structures.array import ArrayStructure
from ..structures.core import StructureFamily
from ..utils import node_repr, path_from_uri
from .array import ArrayAdapter, slice_and_shape_from_block_and_chunks

INLINED_DEPTH = int(os.getenv("TILED_HDF5_INLINED_CONTENTS_MAX_DEPTH", "7"))


def read_zarr(
    data_uri: Union[str, list[str]], structure: Optional[NodeStructure], **kwargs: Any
) -> Union["ZarrGroupAdapter", "ZarrArrayAdapter"]:
    filepath = path_from_uri(data_uri)
    zarr_obj = zarr.open(filepath)  # Group or Array
    adapter: Union[ZarrGroupAdapter, ZarrArrayAdapter]
    if isinstance(zarr_obj, zarr.hierarchy.Group):
        adapter = ZarrGroupAdapter(zarr_obj, **kwargs)
    else:
        if structure is None:
            adapter = ZarrArrayAdapter.from_array(zarr_obj, **kwargs)
        else:
            adapter = ZarrArrayAdapter(zarr_obj, structure=structure, **kwargs)
    return adapter


class ZarrArrayAdapter(ArrayAdapter):
    @classmethod
    def init_storage(cls, data_uri: str, structure: ArrayStructure) -> Any:
        from ..server.schemas import Asset

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

    def _stencil(self) -> tuple[slice, ...]:
        "Trims overflow because Zarr always has equal-sized chunks."
        return tuple(builtins.slice(0, dim) for dim in self.structure().shape)

    def read(self, slice: Optional[slice]) -> NDArray[Any]:
        return self._array[self._stencil()][slice]

    def read_block(
        self, block: Tuple[int, ...], slice: Optional[Union[slice, EllipsisType]]
    ) -> NDArray[Any]:
        block_slice, _ = slice_and_shape_from_block_and_chunks(
            block, self.structure().chunks
        )
        # Slice the block out of the whole array,
        # and optionally a sub-slice therein.
        return self._array[self._stencil()][block_slice][slice]

    def write(
        self,
        data: Union[dask.dataframe.DataFrame, pandas.DataFrame],
        slice: Optional[Union[slice, EllipsisType]],
    ) -> None:
        if slice is not ...:
            raise NotImplementedError
        self._array[self._stencil()] = data

    async def write_block(
        self,
        data: Union[dask.dataframe.DataFrame, pandas.DataFrame],
        block: Tuple[int, ...],
        slice: Optional[Union[slice, EllipsisType]],
    ) -> None:
        if slice is not ...:
            raise NotImplementedError
        block_slice, shape = slice_and_shape_from_block_and_chunks(
            block, self.structure().chunks
        )
        self._array[block_slice] = data


class ZarrGroupAdapter(
    collections.abc.Mapping[str, Union["ZarrArrayAdapter", "ZarrGroupAdapter"]],
    IndexersMixin,
):
    structure_family = StructureFamily.container

    def __init__(
        self,
        node: Any,
        *,
        structure: Optional[Union[NodeStructure, ArrayStructure]] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[list[Spec]] = None,
        access_policy: Optional[Union[SimpleAccessPolicy, DummyAccessPolicy]] = None,
    ) -> None:
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
        return node_repr(self, list(self))

    @property
    def access_policy(self) -> Optional[Union[SimpleAccessPolicy, DummyAccessPolicy]]:
        return self._access_policy

    def metadata(self) -> JSON:
        return self._node.attrs

    def structure(self) -> None:
        return None

    def __iter__(self) -> Iterator[Any]:
        yield from self._node

    def __getitem__(self, key: str) -> Union[ZarrArrayAdapter, "ZarrGroupAdapter"]:
        value = self._node[key]
        if isinstance(value, zarr.hierarchy.Group):
            return ZarrGroupAdapter(value)
        else:
            return ZarrArrayAdapter.from_array(value)

    def __len__(self) -> int:
        return len(self._node)

    def keys(self) -> KeysView:  # type: ignore
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self) -> ValuesView:  # type: ignore
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self) -> ItemsView:  # type: ignore
        return ItemsView(lambda: len(self), self._items_slice)

    def search(self, query: Any) -> None:
        """
        Return a Tree with a subset of the mapping.
        """
        raise NotImplementedError

    def read(self, fields: Optional[str]) -> "ZarrGroupAdapter":
        if fields is not None:
            raise NotImplementedError
        return self

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(self, start: int, stop: int, direction: int) -> list[Any]:
        keys = list(self._node)
        if direction < 0:
            keys = list(reversed(keys))
        return keys[start:stop]

    def _items_slice(self, start: int, stop: int, direction: int) -> list[Any]:
        items = [(key, self[key]) for key in list(self)]
        if direction < 0:
            items = list(reversed(items))
        return items[start:stop]

    def inlined_contents_enabled(self, depth: int) -> bool:
        return depth <= INLINED_DEPTH
