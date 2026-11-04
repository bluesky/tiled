import builtins
from collections.abc import Iterable
from typing import Any

import numpy
from numpy._typing import NDArray

from tiled.adapters.core import Adapter

from ..catalog.orm import Node
from ..ndslice import NDSlice
from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON
from ..utils import path_from_uri
from .resource_cache import with_resource_cache
from .sequence import FileSequenceAdapter
from .utils import force_reshape, init_adapter_from_catalog


class NPYAdapter(Adapter[ArrayStructure]):
    """
    Read the Numpy on-disk format, NPY (.npy).

    Examples
    --------

    >>> NPYAdapter("path/to/file.npy")
    """

    structure_family = StructureFamily.array

    def __init__(
        self,
        data_uri: str,
        structure: ArrayStructure,
        *,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> None:
        """

        Parameters
        ----------
        data_uri :
        structure :
        metadata :
        specs :
        """
        self._filepath = path_from_uri(data_uri)
        super().__init__(structure, metadata=metadata, specs=specs)

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[ArrayStructure],
        node: Node,
        /,
        **kwargs: Any | None,
    ) -> "NPYAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)

    @classmethod
    def from_uris(
        cls,
        data_uri: str,
        **kwargs: Any | None,
    ) -> "NPYAdapter":
        filepath = path_from_uri(data_uri)
        cache_key = (numpy.load, filepath)
        arr = with_resource_cache(cache_key, numpy.load, filepath)

        structure = ArrayStructure(
            shape=arr.shape,
            chunks=tuple((dim,) for dim in arr.shape),
            data_type=BuiltinDtype.from_numpy_dtype(arr.dtype),
        )

        return cls(
            data_uri,
            structure=structure,
        )

    def read(self, slice: NDSlice = NDSlice(...)) -> NDArray[Any]:
        cache_key = (numpy.load, self._filepath)
        arr = with_resource_cache(cache_key, numpy.load, self._filepath)
        arr = force_reshape(arr, self._structure.shape)
        arr = arr[slice] if slice else arr
        return arr

    def read_block(
        self, block: tuple[int, ...], slice: builtins.slice | None = None
    ) -> NDArray[Any]:
        if sum(block) != 0:
            raise IndexError(block)
        cache_key = (numpy.load, self._filepath)
        arr = with_resource_cache(cache_key, numpy.load, self._filepath)
        arr = force_reshape(arr, self._structure.shape)
        arr = arr[slice] if slice else arr
        if slice is not None:
            arr = arr[slice]
        return arr


class NPYSequenceAdapter(FileSequenceAdapter):
    def _load_from_files(
        self, slice: builtins.slice | int | Iterable[int] = slice(None)
    ) -> NDArray[Any]:
        if isinstance(slice, int):
            return numpy.load(self.filepaths[slice])[None, ...]
        else:
            if isinstance(slice, Iterable):
                selected = [self.filepaths[i] for i in slice]
            else:
                selected = self.filepaths[slice]
            return numpy.asarray(self._map_read(selected))

    def _read_one(self, filepath: str) -> NDArray[Any]:
        return numpy.load(filepath)
