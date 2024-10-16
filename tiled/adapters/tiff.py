import builtins
from typing import Any, Dict, List, Optional, Tuple, cast
import warnings

import numpy as np
import tifffile
from numpy._typing import NDArray

from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..utils import path_from_uri
from .protocols import AccessPolicy
from .resource_cache import with_resource_cache
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


def sliced_shape(shp: Tuple[int], slc: Optional[NDSlice] = ...) -> Tuple[int]:
    """Find the shape specification of an array after applying slicing
    """

    if slc is Ellipsis:
        return shp
    if isinstance(slc, int):
        return shp[1:]
    if isinstance(slc, builtins.slice):
        start, stop, step = slc.indices(shp[0])
        return max(0, (stop - start + (step - (1 if step > 0 else -1))) // step), *shp[1:]
    if isinstance(slc, tuple):
        if len(slc) == 0:
            return shp
        else:
            left_axis, *the_rest = slc
            if (left_axis is Ellipsis) and (len(the_rest) < len(shp)-1):
                the_rest.insert(0, Ellipsis)
            return *sliced_shape(shp[:1], left_axis), *sliced_shape(shp[1:], tuple(the_rest))

def force_reshape(arr: np.array, shp: Tuple[int], slc: Optional[NDSlice] = ...):
    """Reshape a numpy array to match the desited shape, if possible.

    Returns
    -------

    A view of the original array
    """

    old_shape = arr.shape
    new_shape = sliced_shape(shp, slc)

    if old_shape == new_shape:
        # Nothing to do here
        return arr
    
    if old_shape.prod() == new_shape.prod():

        if (len(old_shape) != len(new_shape)):
            # Missing or extra unitary dimensions
            warnings.warn(f"Forcefully reshaping {old_shape} to {new_shape}", category=RuntimeWarning)
            return arr.reshape(new_shape)
        else:
            # Some dimensions might be swapped or completely wrong
            # TODO: needs to be treated more carefully

    warnings.warn(f"Can not reshape array of {old_shape} to match {new_shape}; proceeding without changes", category=RuntimeWarning)
    return arr

class TiffSequenceAdapter:
    """ """

    structure_family = "array"

    @classmethod
    def from_uris(
        cls,
        data_uris: List[str],
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "TiffSequenceAdapter":
        """

        Parameters
        ----------
        data_uris :
        structure :
        metadata :
        specs :
        access_policy :

        Returns
        -------

        """
        filepaths = [path_from_uri(data_uri) for data_uri in data_uris]
        seq = tifffile.TiffSequence(filepaths)
        return cls(
            seq,
            structure=structure,
            specs=specs,
            metadata=metadata,
            access_policy=access_policy,
        )

    def __init__(
        self,
        seq: tifffile.TiffSequence,
        *,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """

        Parameters
        ----------
        seq :
        structure :
        metadata :
        specs :
        access_policy :
        """
        self._seq = seq
        # TODO Check shape, chunks against reality.
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        self.access_policy = access_policy
        if structure is None:
            shape = (len(self._seq), *self.read(slice=0).shape)
            structure = ArrayStructure(
                shape=shape,
                # one chunk per underlying TIFF file
                chunks=((1,) * shape[0], *[(i,) for i in shape[1:]]),
                # Assume all files have the same data type
                data_type=BuiltinDtype.from_numpy_dtype(self.read(slice=0).dtype),
            )
        self._structure = structure

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        # TODO How to deal with the many headers?
        return self._provided_metadata

    def read(self, slice: Optional[NDSlice] = ...) -> NDArray[Any]:
        """Return a numpy array

        Receives a sequence of values to select from a collection of tiff files
        that were saved in a folder The input order is defined as: files -->
        vertical slice --> horizontal slice --> color slice --> ... read() can
        receive one value or one slice to select all the data from one file or
        a sequence of files; or it can receive a tuple (int or slice) to select
        a more specific sequence of pixels of a group of images.

        Parameters
        ----------
        slice : NDSlice, optional
            Specification of slicing to be applied to the data array

        Returns
        -------
            Return a numpy array
        """
        if slice is Ellipsis:
            arr = self._seq.asarray()
        elif isinstance(slice, int):
            # e.g. read(slice=0) -- return an entire image
            arr = tifffile.TiffFile(self._seq.files[slice]).asarray()
        elif isinstance(slice, builtins.slice):
            # e.g. read(slice=(...)) -- return a slice along the image axis
            arr = tifffile.TiffSequence(self._seq.files[slice]).asarray()
        elif isinstance(slice, tuple):
            if len(slice) == 0:
                arr = self._seq.asarray()
            elif len(slice) == 1:
                arr = self.read(slice=slice[0])
            else:
                left_axis, *the_rest = slice
                # Could be int or slice (0, slice(...)) or (0,....); the_rest is converted to a list
                if isinstance(left_axis, int):
                    # e.g. read(slice=(0, ....))
                    arr = tifffile.TiffFile(self._seq.files[left_axis]).asarray()
                elif left_axis is Ellipsis:
                    # Return all images
                    arr = self._seq.asarray()
                    the_rest.insert(0, Ellipsis)  # Include any leading dimensions
                elif isinstance(left_axis, builtins.slice):
                    arr = self.read(slice=left_axis)
                arr = force_reshape(arr, self.structure.shape, left_axis)
                arr = np.atleast_1d(arr[tuple(the_rest)])
        else:
            raise RuntimeError(f"Unsupported slice type, {type(clice)} in {slice}")

        return force_reshape(arr, self.structure.shape, slice)

    def read_block(
        self, block: Tuple[int, ...], slice: Optional[NDSlice] = ...
    ) -> NDArray[Any]:
        """

        Parameters
        ----------
        block :
        slice :

        Returns
        -------

        """
        if any(block[1:]):
            raise IndexError(block)
        arr = self.read(builtins.slice(block[0], block[0] + 1))
        return arr[slice]

    def structure(self) -> ArrayStructure:
        """

        Returns
        -------

        """
        return self._structure
