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
from .utils import force_reshape


class _LazyFilepaths:
    """A sequence of filepaths derived lazily from a sequence of data URIs.

    Wraps the `data_uris` given to :class:`FileSequenceAdapter` and converts
    each URI to a local filesystem path (via :func:`path_from_uri`) only when
    that entry is accessed. Deferring the conversion lets the catalog supply a
    fixed-length list in which only the entries a given read needs are populated
    (the rest left as `None`), so a single-frame read out of a large file
    sequence never materializes all N paths. Indexing and slicing mirror a plain
    `list` of paths, so subclasses can treat `self.filepaths` uniformly
    whether the underlying URIs were fully or only partially resolved.
    """

    __slots__ = ("_data_uris",)

    def __init__(self, data_uris: Any) -> None:
        # Stored by reference (not copied) so a caller may populate entries
        # after construction -- as the catalog's lazy per-frame path does once
        # it knows which stack indices the read touches.
        self._data_uris = data_uris

    def __len__(self) -> int:
        return len(self._data_uris)

    def __getitem__(self, key: Union[int, builtins.slice]) -> Any:
        item = self._data_uris[key]
        if isinstance(key, builtins.slice):
            return [path_from_uri(uri) for uri in item]
        return path_from_uri(item)

    def __repr__(self) -> str:
        resolved = sum(uri is not None for uri in self._data_uris)
        return (
            f"<{type(self).__name__} length={len(self._data_uris)} resolved={resolved}>"
        )


class FileSequenceAdapter(Adapter[ArrayStructure]):
    """Base adapter class for image (and other file) sequences

    Assumes that each file contains an array of the same shape and dtype, and the sequence
    of files defines the left-most dimension in the resulting compound (stacked) array.

    If additional reshaping is applied, the `true_shape` derived from the `chunks` attribute
    in the data source properties will reflect the original shape of the stacked array.

    When subclassing, define the `_load_from_files` method specific for a particular file type.

    Parameters
    ----------
    data_uris : Iterable[str]
        A sequence of URIs pointing to the files to be stacked along the left-most dimension.
    structure : Optional[ArrayStructure], optional
        The structure of the resulting array. If not provided, it is inferred from the shape and
        dtype of the first file and the number of files.
    metadata : Optional[JSON], optional
        Additional metadata to be associated with the adapter.
    specs : Optional[List[Spec]], optional
        Additional specs to be associated with the adapter.
    chunks : Optional[tuple[tuple[int, ...], ...]], optional
        A tuple specifying the (true) chunk sizes for each dimension. This is used to determine
        which files to load if additional reshaping is applied. If `chunks` are provided (e.g.
        from data source parameters) they take precedence over the chunks derived from the files.
    """

    # When True, the catalog may build this adapter with a partially-populated
    # list of `data_uris` and resolve only the URIs needed for a given read
    supports_lazy_filepaths = True

    structure_family = StructureFamily.array

    def __init__(
        self,
        data_uris: Optional[Iterable[str]] = None,
        structure: Optional[ArrayStructure] = None,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        chunks: Optional[tuple[tuple[int, ...], ...]] = None,
    ) -> None:
        # `self.filepaths` is a lazy view over the URIs: each is converted to a
        # local path (path_from_uri) only when accessed. `data_uris` is a
        # fixed-length list; the eager case fills every slot, while the catalog's
        # lazy per-frame case leaves the slots it does not need as `None`.
        self.filepaths = _LazyFilepaths(data_uris if data_uris is not None else [])
        self._chunks = chunks  # "True" chunks in the files before reshaping
        if structure is None:
            dat0 = self._load_from_files(0)
            shape = (len(self.filepaths), *dat0.shape[1:])
            structure = ArrayStructure(
                shape=shape,
                # Each file is a single chunk along the left-most dimension;
                # the remaining dimensions are chunked as in the files
                chunks=((1,) * shape[0], *[(i,) for i in shape[1:]]),
                # Assume all files have the same data type
                data_type=BuiltinDtype.from_numpy_dtype(dat0.dtype),
            )
            self._chunks = structure.chunks
        super().__init__(structure, metadata=metadata, specs=specs)

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
        # One entry point for both the eager and lazy paths. The eager path
        # derives `data_uris` from the data source's assets (dense 0-based
        # stacking rank -> uri); the catalog's lazy path passes a sparse
        # `{stack_index: uri}` mapping directly as the `data_uris` kwarg,
        # holding only the frames a given read touches. Uses the `chunks`
        # recorded in the data source properties (set when the stored files are
        # reshaped to the structure shape).
        if "data_uris" not in kwargs:
            kwargs["data_uris"] = cls._stacking_uris_from_assets(data_source)
        kwargs["chunks"] = data_source.properties.get("chunks")
        kwargs["metadata"] = node.metadata_
        kwargs["specs"] = node.specs
        return cls(structure=data_source.structure, **kwargs)

    @staticmethod
    def _stacking_uris_from_assets(
        data_source: DataSource[ArrayStructure],
    ) -> list[str]:
        """Return the list-valued assets' URIs in dense stacking-rank order.

        The data source's assets arrive ordered by `(parameter, num)`, so the
        list-valued ones are already in the stacking order eager reads use. This
        is the sequence-specific analogue of `asset_parameters_to_adapter_kwargs`
        (which the single-file and columnar adapters still use), specialized to
        the one list parameter a file sequence carries.
        """
        return [
            asset.data_uri
            for asset in data_source.assets
            if (asset.num is not None) or (asset.parameter == "data_uris")
        ]

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
        true_shape = tuple(map(sum, self._chunks)) if self._chunks else struct_shape
        is_reshaped = true_shape != struct_shape
        stacking_grid_shape = self._stacking_grid_shape(struct_shape, true_shape)

        if is_reshaped:
            if stacking_grid_shape is None:
                # The reshape interleaves file contents: the per-file boundary
                # falls *inside* a structure dimension, so no subset of whole
                # files can satisfy the read. Load every file, stack, reshape to
                # the structure shape, then apply the requested slice. Correct
                # (row-major order is preserved) but with no partial-load benefit.
                arr = force_reshape(self._load_from_files(), struct_shape)
                arr = np.atleast_1d(arr[slice])
                return force_reshape(arr, ndindex(slice).newshape(struct_shape))

            # The reshape is file-boundary-aligned, so a subset of whole files can
            # satisfy the read (`stacking_grid_shape` is now known non-None).
            expanded = NDSlice(slice).expand_for_shape(struct_shape)
            file_indx_slice = expanded[: len(stacking_grid_shape)]
            # Map the leading (stacking) part of the selection onto flat file indices.
            file_indx_list = NDSlice(file_indx_slice).flat_indices(stacking_grid_shape)

            # The remaining slice to be applied after loading the data from files and stacking;
            # expand to include any non-degenerate leading dimensions along the file axis
            tail_dims_slice = expanded[len(stacking_grid_shape) :]  # noqa: E203
            for slc in file_indx_slice:
                if not isinstance(slc, int):
                    tail_dims_slice = NDSlice(builtins.slice(None), *tail_dims_slice)

            arr = self._load_from_files(slice=file_indx_list)
            stacked_shape = ndindex(file_indx_slice).newshape(struct_shape)
            arr = force_reshape(arr, stacked_shape)
            arr = np.atleast_1d(arr[tail_dims_slice])

            return force_reshape(arr, ndindex(expanded).newshape(struct_shape))

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

    @staticmethod
    def _stacking_grid_shape(
        struct_shape: tuple[int, ...],
        true_shape: tuple[int, ...],
    ) -> Optional[tuple[int, ...]]:
        """The shape of the file-stacking dimensions, or None if not partially-loadable.

        Pure function of the structure shape and the true (stored) shape -- no
        adapter state, no I/O -- so both `read` and the `stack_indices_for_slice`
        classmethod (used by the catalog before any adapter is built) can share it.

        Each file contributes one element along the left-most stored axis, so a
        file spans `P = prod(true_shape[1:])` contiguous elements of the row-major
        flat array. Whole-file (partial) loading is possible only when the file
        boundary aligns with a structure dimension boundary -- i.e. there is a
        split `j` with `prod(struct_shape[:j]) == n_files` -- in which case the
        leading `j` structure dimensions map onto the files and this returns
        `struct_shape[:j]`. When no such split exists the reshape interleaves file
        contents across the file boundary (every file must be loaded) and this
        returns `None`. The caller derives `partial_loadable` as `result is not
        None` and `reshaped` as `true_shape != struct_shape`.
        """
        if math.prod(true_shape) != math.prod(struct_shape):
            raise RuntimeError(
                f"Array with shape {true_shape} derived from storage can not be reshaped "
                f"to match the desired structure, {struct_shape}."
            )
        # Locate the file boundary within the structure shape: the smallest split
        # `j` whose leading dimensions multiply to the file count. Products grow
        # monotonically (dimensions >= 1), so once the running product passes
        # `n_files` without landing on it, no aligned split exists.
        n_files = true_shape[0]
        product = 1
        for j, dim in enumerate(struct_shape, start=1):
            product *= dim
            if product == n_files:
                return struct_shape[:j]
            if product > n_files:
                break
        return None

    @classmethod
    def stack_indices_for_slice(
        cls,
        structure: ArrayStructure,
        chunks: Optional[tuple[tuple[int, ...], ...]],
        slice: Any = ...,
    ) -> Optional[tuple[int, ...]]:
        """Return the global file (stack) indices needed to satisfy `slice`.

        Pure classmethod of the structure and chunks only -- performs no file or
        database I/O and needs no adapter instance -- so the catalog can prefetch
        exactly the assets a read will touch before building any adapter (a
        `read_block` is handled by first converting the block to the equivalent
        slice). The leading `stacking_grid_shape` dimensions of the structure map onto
        the files, so the files touched are the flat positions the slice selects
        within them. Mirrors the file selection in `read()`.

        Returns `None` when the reshape is not file-boundary-aligned (every file
        may need to be loaded), signalling the catalog to skip the lazy loading path.
        """
        struct_shape = structure.shape
        true_shape = tuple(map(sum, chunks)) if chunks else struct_shape
        stacking_grid_shape = cls._stacking_grid_shape(struct_shape, true_shape)
        if stacking_grid_shape is None:
            return None
        file_indx_slice = NDSlice(slice).expand_for_shape(struct_shape)[
            : len(stacking_grid_shape)
        ]
        return NDSlice(file_indx_slice).flat_indices(stacking_grid_shape)
