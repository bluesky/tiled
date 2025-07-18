import builtins
from typing import Any, List, Optional, Set, Tuple, Union

import numpy as np
from numpy._typing import NDArray
from PIL import Image

from ..catalog.orm import Node
from ..ndslice import NDSlice
from ..storage import Storage
from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON
from ..utils import path_from_uri
from .resource_cache import with_resource_cache
from .sequence import FileSequenceAdapter
from .utils import init_adapter_from_catalog


class JPEGAdapter:
    """
    Read a JPEG file.

    Examples
    --------

    >>> JPEGAdapter("path/to/file.jpeg")
    """

    structure_family = StructureFamily.array
    supported_storage: Set[type[Storage]] = set()

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
        filepath = path_from_uri(data_uri)
        cache_key = (Image.open, filepath)
        self._file = with_resource_cache(cache_key, Image.open, filepath)
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
    ) -> "JPEGAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)  # type: ignore

    @classmethod
    def from_uris(
        cls,
        data_uri: str,
        **kwargs: Optional[Any],
    ) -> "JPEGAdapter":
        filepath = path_from_uri(data_uri)
        cache_key = (Image.open, filepath)
        _file = with_resource_cache(cache_key, Image.open, filepath)

        arr = np.asarray(_file)
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
        arr = np.asarray(self._file)
        arr = arr[slice] if slice else arr
        return arr

    def read_block(
        self, block: Tuple[int, ...], slice: Optional[builtins.slice] = None
    ) -> NDArray[Any]:
        if sum(block) != 0:
            raise IndexError(block)

        arr = np.asarray(self._file)
        if slice is not None:
            arr = arr[slice]
        return arr

    def structure(self) -> ArrayStructure:
        return self._structure


class JPEGSequenceAdapter(FileSequenceAdapter):
    def _load_from_files(
        self, slice: Union[builtins.slice, int] = slice(None)
    ) -> NDArray[Any]:
        from PIL import Image

        if isinstance(slice, int):
            return np.asarray(Image.open(self.filepaths[slice]))[None, ...]
        else:
            return np.asarray(
                [np.asarray(Image.open(file)) for file in self.filepaths[slice]]
            )
