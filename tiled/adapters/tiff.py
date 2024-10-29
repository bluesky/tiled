import builtins
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import tifffile
from numpy._typing import NDArray

from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..utils import path_from_uri
from .protocols import AccessPolicy
from .resource_cache import with_resource_cache
from .sequence import FileSequenceAdapter
from .type_alliases import JSON, NDSlice


class TiffAdapter:
    """
    Read a TIFF file.

    Examples
    --------

    >>> TiffAdapter("path/to/file.tiff")
    """

    structure_family = StructureFamily.array

    def __init__(
        self,
        data_uri: str,
        *,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """

        Parameters
        ----------
        data_uri :
        structure :
        metadata :
        specs :
        access_policy :
        """
        if not isinstance(data_uri, str):
            raise Exception
        filepath = path_from_uri(data_uri)
        cache_key = (tifffile.TiffFile, filepath)
        self._file = with_resource_cache(cache_key, tifffile.TiffFile, filepath)
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        self.access_policy = access_policy
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

    def read(self, slice: Optional[NDSlice] = None) -> NDArray[Any]:
        """

        Parameters
        ----------
        slice :

        Returns
        -------

        """
        # TODO Is there support for reading less than the whole array
        # if we only want a slice? I do not think that is possible with a
        # single-page TIFF but I'm not sure. Certainly it *is* possible for
        # multi-page TIFFs.
        arr = self._file.asarray()
        if slice is not None:
            arr = arr[slice]
        return arr

    def read_block(
        self, block: Tuple[int, ...], slice: Optional[slice] = None
    ) -> NDArray[Any]:
        """

        Parameters
        ----------
        block :
        slice :

        Returns
        -------

        """
        # For simplicity, this adapter always treat a single TIFF file as one
        # chunk. This could be relaxed in the future.
        if sum(block) != 0:
            raise IndexError(block)

        arr = self._file.asarray()
        if slice is not None:
            arr = arr[slice]
        return arr

    def structure(self) -> ArrayStructure:
        """

        Returns
        -------

        """
        return self._structure


class TiffSequenceAdapter(FileSequenceAdapter):
    def _load_from_files(
        self, slice: Union[builtins.slice, int] = slice(None)
    ) -> NDArray[Any]:
        return tifffile.TiffSequence(self.filepaths[slice]).asarray()
