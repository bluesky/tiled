import builtins
import math
from abc import abstractmethod
from typing import Any, Iterable, List, Optional, Union

import numpy as np
from ndindex import ndindex
from numpy._typing import NDArray

from tiled.adapters.core import Adapter

from ..catalog.orm import Node
from ..ndslice import NDBlock, NDSlice
from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON, EllipsisType
from ..utils import path_from_uri
from .utils import force_reshape, init_adapter_from_catalog


class FileSequenceAdapter(Adapter[ArrayStructure]):
    """Base adapter class for image (and other file) sequences

    Assumes that each file contains an array of the same shape and dtype, and the sequence
    of files defines the left-most dimension in the resulting compound (stacked) array.

    If additional reshaping is applied, the `true_shape` derived from the `chunks` attribute
    in the data source properties will reflect the original shape of the stacked array.

    When subclassing, define the `_load_from_files` method specific for a particular file type.
    """

    structure_family = StructureFamily.array

    def __init__(
        self,
        data_uris: Iterable[str],
        structure: Optional[ArrayStructure] = None,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        chunks: Optional[tuple[tuple[int]]] = None,
    ) -> None:
        self.filepaths = [path_from_uri(data_uri) for data_uri in data_uris]
        # Keep track of chunks derived from the files themselves, before any reshaping
        self.true_chunks = None
        # TODO Check shape, chunks against reality.
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
            self.true_chunks = structure.chunks
        super().__init__(structure, metadata=metadata, specs=specs)
        # If chunks are provided (e.g., from data source parameters) they take
        # precedence over the chunks derived from the files. This allows for
        # reshaping.
        if chunks is not None:
            self.true_chunks = chunks

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
        adp = init_adapter_from_catalog(cls, data_source, node, **kwargs)
        return adp

    @abstractmethod
    def _load_from_files(
        self, slice: Union[builtins.slice, int, Iterable[int]] = slice(None)
    ) -> NDArray[Any]:
        """Load the array data from files

        Parameters
        ----------
        slice : slice
            an optional slice along the left-most dimension in the resulting array;
            effectively selects a subset of files to be loaded

        Returns
        -------
            A numpy or dask ND array with data from each file stacked along an additional
            (left-most) dimension.
        """

        pass

    def metadata(self) -> JSON:
        # TODO How to deal with the many headers?
        return super().metadata()

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

        # Check if stacked shape and structure shape are compatible; reshape slice if necessary.
        # The shape of the array defined in the structure may not match the actual shape of the
        # stacked files, e.g. if some files are logically grouped along additional dimensions.
        # We assume that all left-most dimensions in the structure beyond the shape of individual
        # files are stacking dimensions, and they define which files need to be read.
        # Finally, the resulting array is reshaped to match the desired structure shape and slice.
        struct_shape = self.structure().shape
        if self.true_chunks:
            true_shape = tuple(map(sum, self.true_chunks))
            if true_shape != struct_shape:
                # The trailing dimensions must match (can be generalized in the future)
                if (math.prod(true_shape) != math.prod(struct_shape)) or (
                    struct_shape[-len(true_shape[1:]) :] != true_shape[1:]  # noqa: E203
                ):
                    raise RuntimeError(
                        f"True shape {true_shape} derived from storage does not "
                        f"match the shape {struct_shape} derived from the structure."
                    )

                # The leading dimensions define stacking and the indices of files we need to read
                stack_shape = struct_shape[: -len(true_shape[1:])]
                slice = NDSlice(slice).expand_for_shape(struct_shape)  # typing: ignore
                file_indx_slice = slice[: len(stack_shape)]
                file_indx_list = (
                    np.arange(true_shape[0])
                    .reshape(stack_shape)[file_indx_slice]
                    .ravel()
                )

                # The remaining slice to be applied after loading the data from files and stacking;
                # expand to include any non-degenerate leading dimensions along the file axis
                tail_dims_slice = slice[len(stack_shape) :]  # noqa: E203
                for slc in file_indx_slice:
                    if not isinstance(slc, int):
                        tail_dims_slice = NDSlice(
                            builtins.slice(None), *tail_dims_slice
                        )

                arr = self._load_from_files(slice=file_indx_list)
                stacked_shape = ndindex(file_indx_slice).newshape(struct_shape)
                arr = force_reshape(arr, stacked_shape)
                arr = np.atleast_1d(arr[tail_dims_slice])

                return force_reshape(arr, ndindex(slice).newshape(struct_shape))

        # Load the data from files, applying the slice along the left-most dimension if possible
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
                    # e.g. read(slice=(0, ....)), dimensionality is reduced by 1
                    arr = np.squeeze(self._load_from_files(left_axis), 0)
                elif left_axis is Ellipsis:
                    # Return all images; include any leading dimensions
                    arr = self._load_from_files()
                    the_rest.insert(0, Ellipsis)
                elif isinstance(left_axis, builtins.slice):
                    # Include the first dimension when further subslicing
                    arr = self.read(slice=left_axis)
                    the_rest.insert(0, builtins.slice(None))

                sliced_shape = ndindex(left_axis).newshape(struct_shape)
                arr = force_reshape(arr, sliced_shape)
                arr = np.atleast_1d(arr[tuple(the_rest)])
        else:
            raise RuntimeError(f"Unsupported slice type, {type(slice)} in {slice}")

        sliced_shape = ndindex(slice).newshape(struct_shape)
        return force_reshape(arr, sliced_shape)

    def read_block(self, block: NDBlock, slice: NDSlice = NDSlice(...)) -> NDArray[Any]:
        if any(block[1:]):
            raise IndexError(block)
        block_slice = block.slice_from_chunks(self._structure.chunks)
        arr = self.read(block_slice[0])
        return arr[slice] if slice else arr
