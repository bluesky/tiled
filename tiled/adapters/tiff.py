import builtins
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

import tifffile
from numpy._typing import NDArray

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


class TiffAdapter:
    """
    Read a TIFF file.

    Examples
    --------

    >>> TiffAdapter("path/to/file.tiff")
    """

    structure_family = StructureFamily.array
    supported_storage: Set[type[Storage]] = set()

    def __init__(
        self,
        data_uri: str,
        *,
        structure: Optional[ArrayStructure] = None,
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
        cache_key = (tifffile.TiffFile, filepath)
        self._file = with_resource_cache(cache_key, tifffile.TiffFile, filepath)
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        if structure is None:
            if self._file.is_shaped:
                from_file: Tuple[Dict[str, Any], ...] = cast(
                    Tuple[Dict[str, Any], ...], self._file.shaped_metadata
                )
                shape = tuple(from_file[0]["shape"])
            else:
                arr = self._file.asarray()
                shape = arr.shape
            structure = ArrayStructure(
                shape=shape,
                chunks=tuple((dim,) for dim in shape),
                data_type=BuiltinDtype.from_numpy_dtype(self._file.series[0].dtype),
            )
        self._structure = structure

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[ArrayStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "TiffAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)  # type: ignore

    @classmethod
    def from_uris(cls, data_uri: str, **kwargs: Optional[Any]) -> "TiffAdapter":
        return cls(data_uri)

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        # This contains some enums, but Python's built-in JSON serializer
        # handles them fine (converting  to str or int as appropriate).
        d = {tag.name: tag.value for tag in self._file.pages[0].tags.values()}
        d.update(self._provided_metadata)
        return d

    def read(self, slice: NDSlice = NDSlice(...)) -> NDArray[Any]:
        # TODO Is there support for reading less than the whole array
        # if we only want a slice? I do not think that is possible with a
        # single-page TIFF but I'm not sure. Certainly it *is* possible for
        # multi-page TIFFs.
        arr = self._file.asarray()
        return arr[slice] if slice else arr

    def read_block(
        self, block: Tuple[int, ...], slice: Optional[slice] = None
    ) -> NDArray[Any]:
        # For simplicity, this adapter always treat a single TIFF file as one
        # chunk. This could be relaxed in the future.
        if sum(block) != 0:
            raise IndexError(block)

        arr = self._file.asarray()
        if slice is not None:
            arr = arr[slice]
        return arr

    def structure(self) -> ArrayStructure:
        return self._structure


class TiffSequenceAdapter(FileSequenceAdapter):
    def _load_from_files(
        self, slice: Union[builtins.slice, int] = slice(None)
    ) -> NDArray[Any]:
        return tifffile.TiffSequence(self.filepaths[slice]).asarray()
