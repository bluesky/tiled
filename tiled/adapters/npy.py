import builtins
from typing import Any, List, Optional, Tuple, Union

import numpy
from numpy._typing import NDArray

from ..catalog.orm import Node
from ..ndslice import NDSlice
from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON
from ..utils import path_from_uri
from .resource_cache import with_resource_cache
from .sequence import FileSequenceAdapter
from .utils import init_adapter_from_catalog


class NPYAdapter:
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
        *,
        structure: ArrayStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
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
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        self._structure = structure

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[ArrayStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "NPYAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)  # type: ignore

    @classmethod
    def from_uris(
        cls,
        data_uri: str,
        **kwargs: Optional[Any],
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

    def metadata(self) -> JSON:
        return self._provided_metadata.copy()

    def read(self, slice: NDSlice = NDSlice(...)) -> NDArray[Any]:
        cache_key = (numpy.load, self._filepath)
        arr = with_resource_cache(cache_key, numpy.load, self._filepath)
        arr = arr[slice] if slice else arr
        return arr

    def read_block(
        self, block: Tuple[int, ...], slice: Optional[builtins.slice] = None
    ) -> NDArray[Any]:
        if sum(block) != 0:
            raise IndexError(block)
        cache_key = (numpy.load, self._filepath)
        arr = with_resource_cache(cache_key, numpy.load, self._filepath)
        arr = arr[slice] if slice else arr
        if slice is not None:
            arr = arr[slice]
        return arr

    def structure(self) -> ArrayStructure:
        return self._structure


class NPYSequenceAdapter(FileSequenceAdapter):
    def _load_from_files(
        self, slice: Union[builtins.slice, int] = slice(None)
    ) -> NDArray[Any]:
        if isinstance(slice, int):
            return numpy.load(self.filepaths[slice])[None, ...]
        else:
            return numpy.asarray([numpy.load(file) for file in self.filepaths[slice]])
