import builtins
import math
import warnings
from abc import abstractmethod
from typing import Any, Iterable, List, Optional, Tuple, Union

import numpy as np
from ndindex import ndindex
from numpy._typing import NDArray

from ..catalog.orm import Node
from ..ndslice import NDSlice
from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON, EllipsisType
from ..utils import path_from_uri
from .utils import init_adapter_from_catalog


def force_reshape(arr: np.array, desired_shape: Tuple[int, ...]) -> np.array:
    """Reshape a numpy array to match the desired shape, if possible.

    Parameters
    ----------

    arr : np.array
        The original ND array to be reshaped
    desired_shape : Tuple[int, ...]
        The desired shape of the resulting array

    Returns
    -------

    A view of the original array
    """

    if arr.shape == desired_shape:
        # Nothing to do here
        return arr

    if arr.size == math.prod(desired_shape):
        if len(arr.shape) != len(desired_shape):
            # Missing or extra singleton dimensions
            warnings.warn(
                f"Forcefully reshaping {arr.shape} to {desired_shape}",
                category=RuntimeWarning,
            )
            return arr.reshape(desired_shape)
        else:
            # Some dimensions might be swapped or completely wrong
            # TODO: needs to be treated more carefully
            pass

    warnings.warn(
        f"Can not reshape array of {arr.shape} to match {desired_shape}; proceeding without changes",
        category=RuntimeWarning,
    )
    return arr


class FileSequenceAdapter:
    """Base adapter class for image (and other file) sequences

    Assumes that each file contains an array of the same shape and dtype, and the sequence of files defines the
    left-most dimension in the resulting compound (stacked) array.

    When subclassing, define the `_load_from_files` method specific for a particular file type.
    """

    structure_family = StructureFamily.array

    def __init__(
        self,
        data_uris: Iterable[str],
        *,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        """

        Parameters
        ----------
        seq :
        structure :
        metadata :
        specs :
        """
        self.filepaths = [path_from_uri(data_uri) for data_uri in data_uris]
        # TODO Check shape, chunks against reality.
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        if structure is None:
            dat0 = self._load_from_files(0)
            shape = (len(self.filepaths), *dat0.shape[1:])
            structure = ArrayStructure(
                shape=shape,
                # one chunk per underlying image file
                chunks=((1,) * shape[0], *[(i,) for i in shape[1:]]),
                # Assume all files have the same data type
                data_type=BuiltinDtype.from_numpy_dtype(dat0.dtype),
            )
        self._structure = structure

    @classmethod
    def from_uris(cls, *data_uris: str) -> "FileSequenceAdapter":
        return cls(data_uris=data_uris)

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[ArrayStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "FileSequenceAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)  # type: ignore

    @abstractmethod
    def _load_from_files(
        self, slice: Union[builtins.slice, int] = slice(None)
    ) -> NDArray[Any]:
        """Load the array data from files

        Parameters
        ----------
        slice : slice
            an optional slice along the left-most dimension in the resulting array; effectively selects a subset of
            files to be loaded

        Returns
        -------
            A numpy or dask ND array with data from each file stacked along an additional (left-most) dimension.
        """

        pass

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        # TODO How to deal with the many headers?
        return self._provided_metadata

    def read(
        self, slice: Union[NDSlice, EllipsisType, builtins.slice] = ...
    ) -> NDArray[Any]:
        """Return a numpy array

        Receives a sequence of values to select from a collection of data files
        that were saved in a folder. The input order is defined as: files -->
        vertical slice --> horizontal slice --> color slice --> ... read() can
        receive one value or one slice to select all the data from one file or
        a sequence of files; or it can receive a tuple (int or slice) to select
        a more specific sequence of pixels of a group of images, for example.

        Parameters
        ----------
        slice : NDSlice, optional
            Specification of slicing to be applied to the data array

        Returns
        -------
            Return a numpy array
        """
        if slice is Ellipsis:
            arr = self._load_from_files()
        elif isinstance(slice, int):
            # e.g. read(slice=0) -- return an entire image (drop 0th dimension of the stack)
            arr = np.squeeze(self._load_from_files(slice), 0)
        elif isinstance(slice, builtins.slice):
            # e.g. read(slice=(...)) -- return a slice along the image axis
            arr = self._load_from_files(slice)
        elif isinstance(slice, tuple):
            if len(slice) == 0:
                arr = self._load_from_files()
            elif len(slice) == 1:
                arr = self.read(slice=slice[0])
            else:
                left_axis, *the_rest = slice
                # Could be int or slice (i, ...) or (slice(...), ...); the_rest is converted to a list
                if isinstance(left_axis, int):
                    # e.g. read(slice=(0, ....))
                    arr = np.squeeze(self._load_from_files(left_axis), 0)
                elif left_axis is Ellipsis:
                    # Return all images
                    arr = self._load_from_files()
                    the_rest.insert(0, Ellipsis)  # Include any leading dimensions
                elif isinstance(left_axis, builtins.slice):
                    arr = self.read(slice=left_axis)

                sliced_shape = ndindex(left_axis).newshape(self.structure().shape)
                arr = force_reshape(arr, sliced_shape)
                arr = np.atleast_1d(arr[tuple(the_rest)])
        else:
            raise RuntimeError(f"Unsupported slice type, {type(slice)} in {slice}")
        sliced_shape = ndindex(slice).newshape(self.structure().shape)
        return force_reshape(arr, sliced_shape)

    def read_block(
        self, block: Tuple[int, ...], slice: NDSlice = NDSlice(...)
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
        return arr[slice] if slice else arr

    def structure(self) -> ArrayStructure:
        """

        Returns
        -------

        """
        return self._structure
