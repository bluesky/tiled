from typing import Any, List, Optional, Tuple, Union

import numpy as np
from numpy._typing import NDArray
from PIL import Image

from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..utils import path_from_uri
from .protocols import AccessPolicy
from .resource_cache import with_resource_cache
from .sequence import FileSequenceAdapter
from .type_alliases import JSON, NDSlice


class JPEGAdapter:
    """
    Read a JPEG file.

    Examples
    --------

    >>> JPEGAdapter("path/to/file.jpeg")
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
        cache_key = (Image.open, filepath)
        self._file = with_resource_cache(cache_key, Image.open, filepath)
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        self.access_policy = access_policy
        if structure is None:
            arr = np.asarray(self._file)
            structure = ArrayStructure(
                shape=arr.shape,
                chunks=tuple((dim,) for dim in arr.shape),
                data_type=BuiltinDtype.from_numpy_dtype(arr.dtype),
            )
        self._structure = structure

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        return self._provided_metadata.copy()

    def read(self, slice: Optional[NDSlice] = None) -> NDArray[Any]:
        """

        Parameters
        ----------
        slice :

        Returns
        -------

        """
        arr = np.asarray(self._file)
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
        if sum(block) != 0:
            raise IndexError(block)

        arr = np.asarray(self._file)
        if slice is not None:
            arr = arr[slice]
        return arr

    def structure(self) -> ArrayStructure:
        """

        Returns
        -------

        """
        return self._structure


class JPEGSequenceAdapter(FileSequenceAdapter):
    def _load_from_files(self, slc: Union[slice, int] = slice(None)) -> NDArray[Any]:
        from PIL import Image

        if isinstance(slc, int):
            return np.asarray(Image.open(self.filepaths[slc]))[None, ...]
        else:
            return np.asarray(
                [np.asarray(Image.open(file)) for file in self.filepaths[slc]]
            )
