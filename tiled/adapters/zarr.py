# mypy: ignore-errors
import builtins
import copy
import os
from importlib.metadata import version
from typing import Any, Iterator, List, Optional, Set, Tuple, Union, cast
from urllib.parse import quote_plus, urljoin, urlparse

import zarr
from numpy._typing import NDArray
from packaging.version import Version

from tiled.adapters.container import ContainerAdapter
from tiled.adapters.core import Adapter
from tiled.structures.container import ContainerStructure

from ..adapters.utils import IndexersMixin
from ..catalog.orm import Node
from ..iterviews import ItemsView, KeysView, ValuesView
from ..ndslice import NDSlice
from ..storage import (
    SUPPORTED_OBJECT_URI_SCHEMES,
    FileStorage,
    ObjectStorage,
    Storage,
    get_storage,
)
from ..structures.array import ArrayStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource
from ..type_aliases import JSON
from ..utils import Conflicts, node_repr, path_from_uri
from .array import ArrayAdapter, slice_and_shape_from_block_and_chunks

ZARR_LIB_V2 = Version(version("zarr")) < Version("3")
if ZARR_LIB_V2:
    from zarr.storage import DirectoryStore as LocalStore
    from zarr.storage import init_array as create_array
else:
    from zarr import create_array
    from zarr.storage import LocalStore, ObjectStore


INLINED_DEPTH = int(os.getenv("TILED_HDF5_INLINED_CONTENTS_MAX_DEPTH", "7"))


class ZarrArrayAdapter(Adapter[ArrayStructure]):
    "Adapter for Zarr arrays"

    structure_family: StructureFamily = StructureFamily.array

    def __init__(
        self,
        array: zarr.Array,
        structure: ArrayStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        self._array = array
        super().__init__(structure, metadata=metadata, specs=specs)

    @classmethod
    def init_storage(
        cls,
        storage: Storage,
        data_source: DataSource[ArrayStructure],
        path_parts: List[str],
    ) -> DataSource[ArrayStructure]:
        data_source = copy.deepcopy(data_source)  # Do not mutate caller input.

        # Zarr requires evenly-sized chunks within each dimension.
        # Use the first chunk along each dimension.
        zarr_chunks = tuple(dim[0] for dim in data_source.structure.chunks)
        shape = tuple(dim[0] * len(dim) for dim in data_source.structure.chunks)
        data_type = data_source.structure.data_type.to_numpy_dtype()
        data_uri = urljoin(
            storage.uri + "/", "/".join(quote_plus(segment) for segment in path_parts)
        )

        if ZARR_LIB_V2:
            zarr_store = LocalStore(str(path_from_uri(data_uri)))
        else:
            zarr_store = ObjectStore(store=storage.get_obstore_location(data_uri))

        create_array(zarr_store, shape=shape, chunks=zarr_chunks, dtype=data_type)

        # Update data source to include the new asset
        data_source.assets.append(
            Asset(
                data_uri=data_uri,
                is_directory=True,
                parameter="data_uri",
            )
        )

        return data_source

    @property
    def dims(self) -> Optional[Tuple[str, ...]]:
        return self._structure.dims

    def _stencil(self) -> Tuple[slice, ...]:
        """Trim overflow because Zarr always has equal-sized chunks."""
        return tuple(builtins.slice(0, dim) for dim in self.structure().shape)

    def get(self, key: str) -> Union[ArrayAdapter, None]:
        return None

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
        arr = cast(NDArray, self._array[self._stencil()])
        return arr[slice]

    def read_block(
        self,
        block: Tuple[int, ...],
        slice: NDSlice = NDSlice(...),
    ) -> NDArray[Any]:
        block_slice, _ = slice_and_shape_from_block_and_chunks(
            block, self.structure().chunks
        )
        # Slice the block out of the whole array,
        # and optionally a sub-slice therein.
        return self._array[self._stencil()][block_slice][slice or ...]

    def write(
        self,
        data: NDArray[Any],
        slice: NDSlice = NDSlice(...),
    ) -> None:
        if slice:
            raise NotImplementedError
        self._array[self._stencil()] = data

    def write_block(
        self,
        data: NDArray[Any],
        block: Tuple[int, ...],
    ) -> None:
        block_slice, shape = slice_and_shape_from_block_and_chunks(
            block, self.structure().chunks
        )
        self._array[block_slice] = data

    def patch(
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
                    f"Slice {slice} does not fit "
                    f"within current array shape {current_shape}. "
                    "Use ?extend=true to extend the array dimensions to fit."
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

    @classmethod
    def supported_storage(cls) -> Set[type[Storage]]:
        return {FileStorage} if ZARR_LIB_V2 else {FileStorage, ObjectStorage}


class ZarrGroupAdapter(
    ContainerAdapter[Union["ArrayAdapter", "ZarrGroupAdapter"]],
    IndexersMixin,
):
    "Adapter for Zarr groups (containers)"

    def __init__(
        self,
        zarr_group: zarr.Group,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        self._zarr_group = zarr_group
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        super().__init__(structure=ContainerStructure(keys=list(self.keys())))

    def __repr__(self) -> str:
        return node_repr(self, list(self))

    def metadata(self) -> dict[str, Any]:
        return (
            {"attributes": self._zarr_group.attrs.asdict()}
            if ZARR_LIB_V2
            else cast(dict[str, Any], self._zarr_group.metadata.to_dict())
        )

    def __iter__(self) -> Iterator[Any]:
        yield from self._zarr_group

    def __getitem__(self, key: str) -> Union[ArrayAdapter, "ZarrGroupAdapter"]:
        value = self._zarr_group[key]
        if isinstance(value, zarr.Group):
            return ZarrGroupAdapter(value)
        else:
            return ArrayAdapter.from_array(value)

    def __len__(self) -> int:
        return len(self._zarr_group)

    def keys(self) -> KeysView:  # type: ignore
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self) -> ValuesView:  # type: ignore
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self) -> ItemsView:  # type: ignore
        return ItemsView(lambda: len(self), self._items_slice)

    def search(self, query: Any) -> None:
        raise NotImplementedError

    def read(self, fields: Optional[str]) -> "ZarrGroupAdapter":
        if fields is not None:
            raise NotImplementedError
        return self

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(
        self, start: int, stop: int, direction: int, page_size: Optional[int] = None
    ) -> List[Any]:
        keys = list(self._zarr_group)
        if direction < 0:
            keys = list(reversed(keys))
        return keys[start:stop]

    def _items_slice(
        self, start: int, stop: int, direction: int, page_size: Optional[int] = None
    ) -> List[Any]:
        items = [(key, self[key]) for key in list(self)]
        if direction < 0:
            items = list(reversed(items))
        return items[start:stop]

    def inlined_contents_enabled(self, depth: int) -> bool:
        return depth <= INLINED_DEPTH


class ZarrAdapter:
    @classmethod
    def from_catalog(
        cls,
        # A Zarr node may reference an array or a group (container).
        data_source: DataSource[Union[ArrayStructure, None]],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> Union[ZarrGroupAdapter, ZarrArrayAdapter]:
        is_container_type = node.structure_family == StructureFamily.container
        uri = data_source.assets[0].data_uri

        if urlparse(uri).scheme == "file":
            # This is a file-based Zarr storage
            zarr_obj = zarr.open(path_from_uri(uri))

        elif urlparse(uri).scheme in SUPPORTED_OBJECT_URI_SCHEMES:
            # This is an object-store-based Zarr storage
            storage = cast(ObjectStorage, get_storage(uri))
            _, _, prefix = storage.parse_blob_uri(uri)
            zarr_store = ObjectStore(store=storage.get_obstore_location())
            # zarr_obj = zarr.open(store=zarr_store)

            if is_container_type:
                zarr_obj = zarr.open_group(store=zarr_store, path=prefix)
            else:
                zarr_obj = zarr.open_array(store=zarr_store, path=prefix)

        else:
            raise TypeError(f"Unsupported URI scheme in {uri}")

        if is_container_type:
            return ZarrGroupAdapter(
                zarr_obj,
                metadata=node.metadata_,
                specs=node.specs,
                **kwargs,
            )
        else:
            return ZarrArrayAdapter(
                zarr_obj,
                structure=cast(ArrayStructure, data_source.structure),
                metadata=node.metadata_,
                specs=node.specs,
                **kwargs,
            )

    @classmethod
    def from_uris(
        cls, data_uri: str, **kwargs: Optional[Any]
    ) -> Union[ZarrArrayAdapter, ZarrGroupAdapter]:
        zarr_obj = zarr.open(path_from_uri(data_uri))  # Group or Array
        if isinstance(zarr_obj, zarr.Group):
            return ZarrGroupAdapter(zarr_obj, **kwargs)
        else:
            structure = ArrayStructure.from_array(zarr_obj)
            return ZarrArrayAdapter(zarr_obj, structure=structure, **kwargs)
