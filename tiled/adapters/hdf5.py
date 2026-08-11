import bisect
import builtins
import copy
import itertools
import os
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import dask
import dask.array
import h5py
import hdf5plugin  # noqa: F401
import numpy
from dask.highlevelgraph import HighLevelGraph
from numpy._typing import NDArray

from tiled.adapters.container import ContainerAdapter
from tiled.structures.container import ContainerStructure

from ..adapters.utils import IndexersMixin
from ..catalog.orm import Node
from ..iterviews import ItemsView, KeysView, ValuesView
from ..ndslice import NDSlice
from ..server.core import NoEntry
from ..structures.array import ArrayStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON
from ..utils import BrokenLink, Sentinel, node_repr, path_from_uri
from .array import ArrayAdapter
from .resource_cache import with_resource_cache
from .sequence import IO_WORKERS, READ_BATCH_BYTES
from .utils import grid_shape_for_files, split_chunks

SWMR_DEFAULT = bool(int(os.getenv("TILED_HDF5_SWMR_DEFAULT", "0")))
INLINED_DEPTH = int(os.getenv("TILED_HDF5_INLINED_CONTENTS_MAX_DEPTH", "7"))

HDF5_DATASET = Sentinel("HDF5_DATASET")
HDF5_BROKEN_LINK = Sentinel("HDF5_BROKEN_LINK")
MIN_CHUNK_SIZE = 1  # Minimum chunk size along the concatenation axis


def _lazy_placeholder_block(
    block_shape: Tuple[int, ...], dtype: numpy.dtype
) -> NDArray:
    """Dask task body for a file that the current read does not touch.

    The catalog's lazy path resolves only the URIs a read needs; the other
    files' blocks carry this placeholder. The read reshapes the full-shape Dask
    array and slices it, so Dask culls every block outside the requested slice
    before computing and never runs these placeholder tasks. If one does run,
    the predicted file geometry (`file_indices_for_slice`) disagreed with the
    blocks that the slice touches, so raise loudly rather than serve wrong data.
    """
    raise RuntimeError(
        "Lazy HDF5 placeholder block was computed: the file geometry predicted "
        "by file_indices_for_slice does not match the blocks the read touches. "
        f"(block_shape={block_shape}, dtype={dtype})"
    )


def parse_hdf5_tree(
    tree: Union[h5py.File, h5py.Group, h5py.Dataset]
) -> Union[dict[str, Union[Any, Sentinel]], Sentinel]:
    """Parse an HDF5 file or group into a nested dictionary structure

    the resulting tree structure represents any groups as nested dictionaries ans datasets as None.

    Parameters
    ----------
    tree : h5py.File or h5py.Group
        The file or group to parse

    Returns
    -------
    dict
        A nested dictionary structure representing the HDF5 file or group
    """
    res: dict[str, Union[Any, None]] = {}

    if isinstance(tree, h5py.Dataset):
        return HDF5_DATASET

    for key, val in tree.items():
        res[key] = HDF5_BROKEN_LINK if val is None else parse_hdf5_tree(val)

    return res


def get_hdf5_attrs(
    file_uri: str,
    dataset: Optional[str] = None,
    swmr: bool = SWMR_DEFAULT,
    libver: str = "latest",
    locking: Optional[Union[bool, str]] = None,
) -> JSON:
    """Get attributes of an HDF5 dataset"""
    file_path = path_from_uri(file_uri)
    with h5open(
        file_path, dataset=dataset, swmr=swmr, libver=libver, locking=locking
    ) as node:
        d = dict(getattr(node, "attrs", {}))
        for k, v in d.items():
            # Convert any bytes to str.
            if isinstance(v, bytes):
                d[k] = v.decode()
    return d


class h5open(h5py.File):  # type: ignore
    """A context manager for reading datasets from HDF5 files

    This class is a subclass of h5py.File that allows for reading datasets from HDF5 files using a context manager.
    It raises a BrokenLink exception if a key referencing a dataset (or a group) exists in the file, but the
    referenced object can not be accessed (e.g. if an externally linked file has been removed). In these cases,
    h5py raises a KeyError with following messages:
    KeyError: 'Unable to synchronously open object (component not found)'
    or
    KeyError: "Unable to synchronously open object (unable to open external file, external link file name = '...')"
    KeyError: "Unable to synchronously open object (can't open file)"
    if a soft link or an external link is broken, respectively.

    This message is distinct from the case when a key does not exist in the file, in which case h5py raises:
    KeyError: "Unable to synchronously open object (object 'y' doesn't exist)"
    """

    def __init__(
        self, filename: Union[str, Path], dataset: Optional[str] = None, **kwargs: Any
    ) -> None:
        super().__init__(filename, mode="r", **kwargs)
        self.dataset = dataset

    def __enter__(self) -> Union[h5py.File, h5py.Group, h5py.Dataset]:
        super().__enter__()
        try:
            return self[self.dataset] if self.dataset else self
        except Exception:
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:  # type: ignore
        super().__exit__(exc_type, exc_value, exc_tb)

        if exc_type == KeyError:
            if "file" in str(exc_value):
                # External link is broken
                raise BrokenLink(exc_value.args[0]) from exc_value

            elif "component not found" in str(exc_value):
                # Soft link is broken
                raise BrokenLink(exc_value.args[0]) from exc_value


class HDF5ArrayAdapter(ArrayAdapter):
    """Adapter for array-type data stored in HDF5 files

    This adapter lazily loads array data from HDF5 files using Dask. Supports reading from datasets spanning
    multiple files.
    """

    # When True, the catalog may build this adapter with a partially-populated
    # list of `data_uris`: it resolves only the files that a given read touches
    # and leaves the rest as None, then resolves the untouched files' blocks
    # lazily.
    supports_lazy_assets = True

    @staticmethod
    def _file_layout(
        structure: ArrayStructure,
        n_files: int,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[str, Tuple[int, ...]]]:
        """Describe how the `n_files` backing files tile the array's leading axes.

        HDF5 files are CONCATENATED along the array's leading (axis-0) stored
        dimension, and each file may contribute a DIFFERENT number of elements
        there (unlike a TIFF stack, where every file adds exactly one frame on a
        new axis). The combined leading chunking is then non-uniform, and file
        boundaries can NOT be recovered from `structure.chunks` alone. This
        returns one of:

        - `("extents", extents)` -- file `i` is a contiguous slab of `extents[i]`
          elements along structure axis 0; `sum(extents) == shape[0]`. Cumulative
          sums give the file boundaries, so any per-file extents (uniform or not)
          are supported. Sourced from the optional `properties["extents"]` (the
          authoritative per-asset row counts the writer stored) or, absent that,
          from `structure.chunks[0]` when it has exactly one chunk per file (each
          file is then one leading chunk).
        - `("grid", grid_shape)` -- the leading `grid_shape` dimensions (product
          `== n_files`) each index one file (a uniform stack whose leading axis
          was reshaped across several structure dimensions). Used only when the
          per-file extents can not be determined as above.
        - `None` -- files can not be located from the structure, so the caller
          must skip the lazy path and open every file.
        """
        struct_shape = tuple(structure.shape)
        raw_extents = (properties or {}).get("extents")
        if raw_extents is not None:
            extents = tuple(int(e) for e in raw_extents)
            if len(extents) != n_files or sum(extents) != struct_shape[0]:
                # Inconsistent with the structure: don't trust it for selection.
                return None
            return ("extents", extents)
        chunks0 = tuple(int(c) for c in structure.chunks[0])
        if len(chunks0) == n_files:
            # One leading chunk per file: the chunk sizes ARE the per-file extents
            # (holds when each file was written as a single leading chunk).
            return ("extents", chunks0)
        grid_shape = grid_shape_for_files(struct_shape, n_files)
        if grid_shape is not None:
            return ("grid", grid_shape)
        return None

    @classmethod
    def file_indices_for_slice(
        cls,
        structure: ArrayStructure,
        n_files: int,
        slice: Any = ...,
        properties: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[int, ...]]:
        """Return the global file indices needed to satisfy `slice`.

        A pure classmethod of the structure, the file count and the data source
        `properties`/`parameters` -- it touches no file or database and needs no
        adapter instance -- so the catalog can prefetch exactly the assets that a
        read will touch before building any adapter (for a `read_block`, the
        catalog first converts the block to the equivalent slice). It mirrors the
        file selection in the lazy `read()`.

        `n_files` is the number of backing files (the catalog passes the asset
        count). `_file_layout` locates the files within the structure; this
        maps the slice onto them: for `("extents", extents)`, the slice's axis-0
        selection is mapped through the cumulative file boundaries; for
        `("grid", grid_shape)`, the leading grid dimensions index the files
        directly.

        Returns `None` when the files can not be located -- so a slice may need
        every file -- to tell the catalog to skip the lazy path. This includes
        the case of a `slice`/`squeeze` adapter parameter: those transforms
        reshape each file when building the served array (see `from_catalog`), so
        the served axis 0 is no longer a plain concatenation of whole files and
        the requested slice (in served coordinates) can not be mapped back onto
        the stored per-file boundaries. `_file_layout` alone can not detect this
        -- `structure.chunks[0]` may still look one-per-file -- so it is caught
        here from the data source `parameters`.
        """
        parameters = parameters or {}
        if parameters.get("slice") is not None or parameters.get("squeeze"):
            return None
        layout = cls._file_layout(structure, n_files, properties)
        if layout is None:
            return None
        struct_shape = tuple(structure.shape)
        expanded = NDSlice(slice).expand_for_shape(struct_shape)
        kind, data = layout
        if kind == "grid":
            file_indx_slice = expanded[: len(data)]
            return NDSlice(file_indx_slice).flat_indices(data)
        # kind == "extents": map the axis-0 selection through the file boundaries.
        ends = tuple(itertools.accumulate(data))  # exclusive end position per file
        axis0 = expanded[0]
        if isinstance(axis0, builtins.slice):
            positions: Iterable[int] = range(*axis0.indices(struct_shape[0]))
        else:
            positions = (int(axis0),)
        # bisect_right(ends, p) is the index of the file whose half-open
        # [start, end) range contains flat position p.
        return tuple(sorted({bisect.bisect_right(ends, p) for p in positions}))

    @staticmethod
    def _lazy_stack_from_structure(
        data_uris: Tuple[Optional[str], ...],
        structure: ArrayStructure,
        dataset: Optional[str] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: Optional[Union[bool, str]] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> dask.array.Array:
        """Build the file-stacked Dask array from the known structure, opening no
        file for specs.

        The catalog knows the structure, so a read need not open any file to
        discover shapes: `data_uris` carries one entry per backing file (its
        length is the file count), populated only for the files that this read
        touches and left `None` elsewhere. Each present URI is converted to a
        local path (via `path_from_uri`) lazily, inside its read task, so an
        untouched file is never resolved. `_file_layout` gives each file's
        extent along a single leading axis; the remaining dimensions are that
        file's own shape. The array is built one whole file per block along that
        axis, then reshaped to the structure by the caller's `read` (via
        `force_reshape`), so Dask culls every untouched file's block before
        computing.

        A present entry reads its whole file in one task (reshaping it to the
        block's shape, which absorbs a stacking axis or a per-file row count
        equally); a `None` entry gets a placeholder that raises if ever computed
        (it should always be culled).
        """
        n_files = len(data_uris)
        dtype = structure.data_type.to_numpy_dtype()
        struct_shape = tuple(structure.shape)
        layout = HDF5ArrayAdapter._file_layout(structure, n_files, properties)
        if layout is None:
            # The catalog only takes the lazy path when the files can be located,
            # so this should not happen; guard rather than build a wrong graph.
            raise ValueError(
                f"Structure shape {struct_shape} does not locate {n_files} files."
            )
        kind, data = layout
        if kind == "extents":
            # File i is `data[i]` elements along axis 0; the rest is its own shape.
            leading_extents = data
            per_file_rest = struct_shape[1:]
        else:  # "grid": each file is one element on a flattened leading axis.
            leading_extents = (1,) * n_files
            per_file_rest = struct_shape[len(data) :]  # noqa: E203

        def _read_hdf5_file(uri: str, block_shape: Tuple[int, ...]) -> NDArray:
            # Resolve the URI to a local path only now, when this file is read.
            with h5open(
                path_from_uri(uri), dataset, swmr=swmr, libver=libver, locking=locking
            ) as ds:
                # Read the whole file; reshape it into this file's block (adds a
                # stacking axis when the extent is 1, or is a no-op for a concat).
                return numpy.asarray(ds[...]).reshape(block_shape)

        name = "hdf5-lazy-stack-" + str(
            hash((dataset, data_uris, struct_shape, str(dtype)))
        )
        dsk: dict[Any, Any] = {}
        rest_key = tuple(0 for _ in per_file_rest)
        for file_indx, (uri, extent) in enumerate(zip(data_uris, leading_extents)):
            block_shape = (extent, *per_file_rest)
            key = (name, file_indx, *rest_key)
            if uri is None:
                dsk[key] = (_lazy_placeholder_block, block_shape, dtype)
            else:
                dsk[key] = (_read_hdf5_file, uri, block_shape)

        chunks_final = (tuple(leading_extents), *[(s,) for s in per_file_rest])
        hlg = HighLevelGraph.from_collections(name, dsk, dependencies=[])
        return dask.array.Array(hlg, name, chunks=chunks_final, dtype=dtype)

    @staticmethod
    def lazy_load_hdf5_array(
        *file_paths: Union[str, Path],
        dataset: Optional[str] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: Optional[Union[bool, str]] = None,
    ) -> dask.array.Array:
        """Lazily load arrays from possibly multiple HDF5 files and concatenate them along the first axis

        The chunks of the resulting Dask array are determined by the chunks of the constituent arrays.

        Every file is opened to read its specs (shapes/chunks/dtype). When the
        structure is already known (a catalog read), prefer
        `_lazy_stack_from_structure`, which opens no file for specs and can leave
        untouched files unresolved.

        Parameters
        ----------
        file_paths : list
            A list of file paths pointing to the HDF5 files
        dataset : str
            The dataset to read from the files, for example, "/path/to/dataset" within the file
        swmr : bool
            Whether to open the files in single-writer multiple-reader mode
        libver : str
            The HDF5 library version to use
        locking : bool
            Whether to use file locking when accessing the files
        """

        # Define helper functions for reading and getting specs of HDF5 arrays
        def _read_hdf5_array(
            fpath: Union[str, Path], slice: tuple[builtins.slice, ...]
        ) -> NDArray:
            with h5open(
                fpath, dataset, swmr=swmr, libver=libver, locking=locking
            ) as ds:
                return ds[slice]

        def _get_hdf5_specs(
            fpath: Union[str, Path]
        ) -> Tuple[Tuple[int, ...], Tuple[int, ...], numpy.dtype]:
            with h5open(
                fpath, dataset, swmr=swmr, libver=libver, locking=locking
            ) as ds:
                result = ds.shape, ds.chunks or ds.shape, ds.dtype
            return result

        # Need to know shapes/dtypes of constituent arrays to load them lazily.
        # Reading specs opens every file (twice, for external links), so for
        # many-file datasets do this in parallel while preserving order.
        if len(file_paths) > 1:
            with ThreadPoolExecutor(
                max_workers=min(IO_WORKERS, len(file_paths))
            ) as executor:
                shapes_chunks_dtypes = list(executor.map(_get_hdf5_specs, file_paths))
        else:
            shapes_chunks_dtypes = [_get_hdf5_specs(fpath) for fpath in file_paths]
        dtype = shapes_chunks_dtypes[0][2]
        if dtype == numpy.dtype("O"):
            # h5py uses NumPy's object dtype to represent variable-length
            # strings, vlen arrays, and HDF5 references. These are a
            # Python-only h5py feature not supported by HDF5 in general.
            # See https://docs.h5py.org/en/stable/special.html.
            #
            # Variable-length strings are repackaged into a fixed-length
            # bytes array (which dask can chunk). For any other object
            # dtype we serve an empty placeholder preserving the original
            # shape, since we cannot generally read it as a numpy array
            # (and dask cannot auto-chunk object dtype).
            warnings.warn(
                f"The dataset {dataset} is of object type, using a "
                "Python-only feature of h5py that is not supported by "
                "HDF5 in general. Read more about that feature at "
                "https://docs.h5py.org/en/stable/special.html. "
                "Consider using a fixed-length field instead. "
                "If the data are variable-length strings, Tiled will "
                "repackage them as a fixed-length bytes array; "
                "otherwise an empty placeholder of the same shape "
                "will be served."
            )

            is_vlen_string = h5py.check_string_dtype(dtype) is not None

            def _read_as_bytes(fpath: Union[str, Path]) -> NDArray:
                with h5open(
                    fpath, dataset, swmr=swmr, libver=libver, locking=locking
                ) as ds:
                    if is_vlen_string:
                        # Coerce vlen-string object array to fixed-length bytes
                        return numpy.asarray(ds[()], dtype=bytes)
                    # Non-string object dtype: serve an empty placeholder
                    # of the same shape (zero-length bytes).
                    return numpy.empty(ds.shape, dtype="S0")

            arrays = [_read_as_bytes(fp) for fp in file_paths]
            # Mirror the empty/scalar vs. multi-dim behavior of the
            # non-object branch below: stack (adding a leading axis) only
            # for true scalars; otherwise concatenate along axis 0.
            if arrays[0].shape == () or 0 in arrays[0].shape:
                stacked = numpy.stack(arrays)
            elif len(arrays) > 1:
                stacked = numpy.concatenate(arrays, axis=0)
            else:
                stacked = arrays[0]
            return dask.array.from_array(stacked, chunks=stacked.shape)

        if all((not shp) or (0 in shp) for shp, _, _ in shapes_chunks_dtypes):
            # Treat empty arrays and scalars separately: all shapes are empty or has 0
            array = dask.array.stack([_read_hdf5_array(fp, ()) for fp in file_paths])

        else:
            # Use delayed loading to read and conactenate arrays from multiple files
            # First, find chunks along the left axis (split per file),
            # and chunks in the rest of the dimensions (same for each file)
            file_chunks = tuple(
                split_chunks(shp[0], max(chk[0], MIN_CHUNK_SIZE))
                for shp, chk, _ in shapes_chunks_dtypes
            )
            # Shape of the rest of dimensions
            rest_shape = shapes_chunks_dtypes[0][0][1:]
            rest_chunks = tuple(
                split_chunks(shp, min(chk[i + 1] for _, chk, _ in shapes_chunks_dtypes))
                for i, shp in enumerate(rest_shape)
            )
            dim0_chunks = tuple(size for fc in file_chunks for size in fc)
            chunks_final = (dim0_chunks, *[tuple(d) for d in rest_chunks])

            # Prepare slice tuples and indices for the rightmost dimensions (same for each file)
            key_rest: Tuple[Tuple[int, ...], ...]
            slc_rest: Tuple[Tuple[builtins.slice, ...], ...]
            if not rest_chunks or (max(len(dim) for dim in rest_chunks) == 1):
                # All dimensions have only one chunk per each: use full slices
                key_rest = (tuple(0 for _ in rest_chunks),)
                slc_rest = (tuple(builtins.slice(None) for _ in rest_chunks),)
            else:
                # Multiple chunks in at least one of the dimensions:
                # build full product of indices and corresponding slices
                key_rest = tuple(
                    itertools.product(*(range(len(dim)) for dim in rest_chunks))
                )
                rest_bounds = tuple(
                    numpy.cumsum((0,) + dim).tolist() for dim in rest_chunks
                )
                rest_starts = itertools.product(
                    *(bounds[:-1] for bounds in rest_bounds)
                )
                rest_stops = itertools.product(*(bounds[1:] for bounds in rest_bounds))
                slc_rest = tuple(
                    tuple(
                        builtins.slice(start, stop)
                        for start, stop in zip(dim_starts, dim_stops)
                    )
                    for dim_starts, dim_stops in zip(rest_starts, rest_stops)
                )

            # Define the Dask tasks for loading each chunk from the files
            name = "hdf5-stack-" + str(hash(tuple([dataset, *file_paths])))
            dsk = {}  # mapping of (name: task + args) for delayed read task
            dim0_chunk_idx = 0  # global chunk index along the leftmost dimension
            for fpath, dim0_chunks in zip(file_paths, file_chunks):
                # Main loop over the chunks for the leftmost dimension for each file
                dim0_start = 0
                for dim0_chunk_size in dim0_chunks:
                    dim0_stop = dim0_start + dim0_chunk_size
                    slc = (builtins.slice(dim0_start, dim0_stop),)
                    key = (name, dim0_chunk_idx)

                    # Inner loop over the rest of dimensions
                    for kr, sr in zip(key_rest, slc_rest):
                        dsk[key + kr] = (_read_hdf5_array, fpath, slc + sr)

                    dim0_start = dim0_stop
                    dim0_chunk_idx += 1

            # Build the high-level graph and the resulting Dask array
            hlg = HighLevelGraph.from_collections(name, dsk, dependencies=[])
            array = dask.array.Array(hlg, name, chunks=chunks_final, dtype=dtype)

        return array

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[ArrayStructure],
        node: Node,
        /,
        dataset: Optional[str] = None,
        slice: Optional[Union[str, NDSlice]] = None,
        squeeze: Optional[bool] = False,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: Optional[Union[bool, str]] = None,
        data_uris: Optional[List[Optional[str]]] = None,
    ) -> "HDF5ArrayAdapter":
        structure = data_source.structure

        if data_uris is not None:
            # Lazy path: the catalog resolved only the files that this read touches
            # (leaving the rest as None) and passes them here. The structure is
            # known, so build the file-stacked array without opening any file for
            # specs; placeholders stand in for the untouched files' blocks, which
            # Dask culls once the read slices the reshaped array. Each URI is
            # converted to a local path lazily, inside its read task, so untouched
            # files stay closed. Metadata comes from the node alone (no attribute
            # read).
            array = cls._lazy_stack_from_structure(
                tuple(data_uris),
                structure,
                dataset=dataset,
                swmr=swmr,
                libver=libver,
                locking=locking,
                properties=data_source.properties,
            )
            if slice:
                if isinstance(slice, str):
                    slice = NDSlice.from_numpy_str(slice)
                array = array[slice]
            if squeeze:
                array = array.squeeze()
            return cls(
                array,
                structure,
                metadata=copy.deepcopy(node.metadata_),
                specs=node.specs,
            )

        assets = data_source.assets
        asset_uris = [
            ast.data_uri for ast in assets if ast.parameter == "data_uris"
        ] or [assets[0].data_uri]
        file_paths = [path_from_uri(uri) for uri in asset_uris]

        # Building the lazy Dask array opens every constituent file to read its
        # specs. Cache the array so repeated reads of the same dataset reuse the
        # graph instead of re-opening all files. Slicing the cached array below
        # returns a new array, leaving the cached one unmodified.
        cache_key = (
            HDF5ArrayAdapter.lazy_load_hdf5_array,
            dataset,
            tuple(file_paths),
            swmr,
            libver,
            locking,
        )
        array = with_resource_cache(
            cache_key,
            cls.lazy_load_hdf5_array,
            *file_paths,
            dataset=dataset,
            swmr=swmr,
            libver=libver,
            locking=locking,
        )

        if slice:
            if isinstance(slice, str):
                slice = NDSlice.from_numpy_str(slice)
            array = array[slice]
        if squeeze:
            array = array.squeeze()

        if array.dtype != structure.data_type.to_numpy_dtype():
            raise ValueError(
                f"Data type mismatch between array data and structure: "
                f"{array.dtype} != {structure.data_type.to_numpy_dtype()}"
            )

        # TODO: Possibly rechunk according to structure.chunks? Is it expensive/necessary?
        # array = dask.array.rechunk(array, chunks=structure.chunks)

        # Pull additional metadata from the file attributes
        metadata = copy.deepcopy(node.metadata_)
        metadata.update(
            get_hdf5_attrs(
                asset_uris[0], dataset, swmr=swmr, libver=libver, locking=locking
            )
        )

        return cls(
            array,
            structure,
            metadata=metadata,
            specs=node.specs,
        )

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        dataset: Optional[str] = None,
        slice: Optional[Union[str, NDSlice]] = None,
        squeeze: bool = False,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: Optional[Union[bool, str]] = None,
    ) -> "HDF5ArrayAdapter":
        file_paths = [path_from_uri(uri) for uri in data_uris]

        array = cls.lazy_load_hdf5_array(
            *file_paths, dataset=dataset, swmr=swmr, libver=libver, locking=locking
        )

        # Apply slice and squeeze operations, if specified
        if slice:
            if isinstance(slice, str):
                slice = NDSlice.from_numpy_str(slice)
            array = array[slice]
        if squeeze:
            array = array.squeeze()

        # Construct the structure and pull additional metadata from the file attributes
        structure = ArrayStructure.from_array(array)
        metadata = get_hdf5_attrs(
            data_uris[0], dataset, swmr=swmr, libver=libver, locking=locking
        )

        return cls(array, structure, metadata=metadata)


class HDF5Adapter(
    ContainerAdapter[Union["HDF5Adapter", HDF5ArrayAdapter]], IndexersMixin
):
    """Adapter for HDF5 files

    This map the structure of an HDF5 file onto a "Tree" of array structures.

    Parameters
    ----------
    tree : dict
        A dictionary representing the HDF5 file or group. The keys are the names of the groups or datasets,
        and the values are either dictionaries (representing groups) or None (representing datasets).
        HDF5 datasets will be mapped to HDF5ArrayAdapter instances, and groups will be mapped to HDF5Adapter
        instances. The tree is rooted at the 'dataset' node.
    data_uris : str
        The URI of the file, or a list of URIs if the dataset spans multiple files.
    dataset : str
        The dataset to read, for example, "/path/to/dataset" within the file. If supplied, this path will
        effectively become the root of the adapter.
    metadata : dict
        Metadata for the adapter
    specs : list
        A list of specs for the adapter
    kwargs : dict
        Additional keyword arguments, such as swmr, libver, etc. -- they are not stored as separate attributes

    Examples
    --------

    From the root node of a file given a filepath

    >>> import h5py
    >>> HDF5Adapter.from_uri("file://localhost/path/to/file.h5")

    """

    # The catalog serves an HDF5 array node through this container adapter (the
    # mimetype-registered class), which dispatches array data sources to
    # HDF5ArrayAdapter. Opt in to the catalog's lazy per-frame path here and
    # delegate the file-selection geometry to the array adapter; the catalog
    # consults both only when reading an array data source.
    supports_lazy_assets = True

    @classmethod
    def file_indices_for_slice(
        cls,
        structure: ArrayStructure,
        n_files: int,
        slice: Any = ...,
        properties: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[int, ...]]:
        return HDF5ArrayAdapter.file_indices_for_slice(
            structure, n_files, slice, properties, parameters
        )

    def __init__(
        self,
        tree: Union[dict[str, Any], Sentinel],
        *data_uris: str,
        dataset: Optional[str] = None,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        **kwargs: Optional[Any],
    ) -> None:
        if tree == HDF5_BROKEN_LINK:
            raise BrokenLink(
                f"Unable to open object at {data_uris[0]}"
                + (f"/{dataset}" if dataset else "")
            )
        self._tree: dict[str, Any] = tree  # type: ignore
        self.uris = data_uris
        self.dataset = dataset  # Referenced to the root of the file
        self._kwargs = kwargs  # e.g. swmr, libver, locking, etc.
        super().__init__(
            structure=ContainerStructure(keys=list(self._tree.keys())),
            metadata=metadata,
            specs=specs,
        )

    @classmethod
    def from_catalog(
        cls,
        # An HDF5 node may reference a dataset (array) or group (container).
        data_source: DataSource[Union[ArrayStructure, None]],
        node: Node,
        /,
        dataset: Union[str, list[str]] = "/",
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: Optional[Union[bool, str]] = None,
        **kwargs: Any,  # Optional kwargs for HDF5ArrayAdapter
    ) -> Union["HDF5Adapter", HDF5ArrayAdapter]:
        if not isinstance(dataset, str):
            dataset = "/".join(dataset)

        # If the data source is an array, return an HDF5ArrayAdapter
        if data_source.structure_family == StructureFamily.array:
            return HDF5ArrayAdapter.from_catalog(
                data_source,  # type: ignore
                node,
                dataset=dataset,
                swmr=swmr,
                libver=libver,
                locking=locking,
                **kwargs,
            )

        # Initialize adapter for the entire HDF5 tree
        # If multiple data_uri assets are supplied, only the first one is traversed, but the rest of the uris
        # will be kept in case we need to read an array that spans all of them.
        assets = data_source.assets
        assert len(assets) > 0, "No assets found in data source"
        data_uris = [
            ast.data_uri for ast in assets if ast.parameter == "data_uris"
        ] or [assets[0].data_uri]
        file_path = path_from_uri(data_uris[0])
        with h5open(
            file_path, dataset, swmr=swmr, libver=libver, locking=locking
        ) as file:
            tree = parse_hdf5_tree(file)

        if tree == HDF5_DATASET:
            raise ValueError(
                "Erroneous structure (container) of a DataSource pointing to an HDF5 Dataset (array)."
            )

        return cls(
            tree,
            *data_uris,
            dataset=dataset,
            structure=data_source.structure,
            metadata=node.metadata_,
            specs=node.specs,
            swmr=swmr,
            libver=libver,
            locking=locking,
        )

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        dataset: Optional[str] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: Optional[Union[bool, str]] = None,
        **kwargs: Any,  # Optional kwargs for HDF5ArrayAdapter
    ) -> Union["HDF5Adapter", HDF5ArrayAdapter]:
        fpath = path_from_uri(data_uris[0])
        with h5open(fpath, dataset, swmr=swmr, libver=libver, locking=locking) as file:
            tree = parse_hdf5_tree(file)

        if tree == HDF5_DATASET:
            return HDF5ArrayAdapter.from_uris(
                *data_uris,
                dataset=dataset,
                swmr=swmr,
                libver=libver,
                locking=locking,
                **kwargs,
            )

        return cls(
            tree, *data_uris, dataset=dataset, swmr=swmr, libver=libver, locking=locking
        )

    def __repr__(self) -> str:
        return node_repr(self, list(self))

    def metadata(self) -> JSON:
        d = get_hdf5_attrs(self.uris[0], self.dataset)
        return {**d, **super().metadata()}

    def __iter__(self) -> Iterator[Any]:
        """Iterate over the keys of the tree"""
        yield from self._tree

    def __getitem__(self, key: str) -> Union["HDF5Adapter", HDF5ArrayAdapter]:
        dataset = f"{self.dataset or ''}/{key.strip('/')}"  # Referenced to the root of the file
        node = copy.deepcopy(self._tree)
        for segment in key.strip("/").split("/"):
            if segment not in node:
                raise NoEntry(
                    f"Can not access dataset {dataset} in {self.uris[0]}: {key} not found"
                )
            node = node[segment]
            if node == HDF5_BROKEN_LINK:  # type: ignore
                raise BrokenLink(f"Unable to open object at {self.uris[0]}/{dataset}")
        if isinstance(node, dict):
            # It is an HDF5 group
            return HDF5Adapter(
                node,
                *self.uris,
                dataset=dataset,
                metadata=self._metadata,
                specs=self.specs,
                **self._kwargs,
            )
        else:
            # It is an HDF5 dataset
            return HDF5ArrayAdapter.from_uris(
                *self.uris, dataset=dataset, **self._kwargs
            )

    def get(self, key: str, *args: Any) -> Union["HDF5Adapter", HDF5ArrayAdapter]:
        """Overwrite to always raise KeyErrors for broken links and missing items"""
        return self[key]

    def __len__(self) -> int:
        return len(self._tree)

    def keys(self) -> KeysView:  # type: ignore
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self) -> ValuesView:  # type: ignore
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self) -> ItemsView:  # type: ignore
        return ItemsView(lambda: len(self), self._items_slice)

    def search(self, query: Any) -> None:
        raise NotImplementedError

    def read(self, fields: Optional[str] = None) -> "HDF5Adapter":
        if fields is not None:
            raise NotImplementedError
        return self

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(
        self, start: int, stop: int, direction: int, page_size: Optional[int] = None
    ) -> List[Any]:
        keys = list(self._tree.keys())
        if direction < 0:
            keys = list(reversed(keys))
        return keys[start:stop]

    def _items_slice(
        self, start: int, stop: int, direction: int, page_size: Optional[int] = None
    ) -> List[Tuple[Any, Any]]:
        """

        Parameters
        ----------
        start :
        stop :
        direction :

        Returns
        -------

        """
        items = [(key, self[key]) for key in list(self)]
        if direction < 0:
            items = list(reversed(items))
        return items[start:stop]

    def inlined_contents_enabled(self, depth: int) -> bool:
        return depth <= INLINED_DEPTH
