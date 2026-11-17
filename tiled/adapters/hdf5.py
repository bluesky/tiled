import builtins
import copy
import functools
import itertools
import os
import posixpath
import sys
import threading
import warnings
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any

import cachetools
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
from .utils import split_chunks

SWMR_DEFAULT = bool(int(os.getenv("TILED_HDF5_SWMR_DEFAULT", "0")))
INLINED_DEPTH = int(os.getenv("TILED_HDF5_INLINED_CONTENTS_MAX_DEPTH", "7"))

HDF5_DATASET = Sentinel("HDF5_DATASET")
HDF5_BROKEN_LINK = Sentinel("HDF5_BROKEN_LINK")
MIN_CHUNK_SIZE = 1  # Minimum chunk size along the concatenation axis

# Limits on following a virtual dataset through its sources.
MAX_VIRTUAL_DEPTH = 16
MAX_VIRTUAL_SOURCES = 10_000
# Virtual datasets whose sources were checked, with the state of every file
# that the result depends on, as it was when the file was looked at.
FileState = tuple[int, int, int, int] | None
_CHECKED_VIRTUAL: cachetools.LRUCache[
    tuple[str, str, tuple[Path, ...]], list[tuple[str, FileState]]
] = cachetools.LRUCache(maxsize=1024)
_CHECKED_VIRTUAL_LOCK = threading.Lock()


def _resolve_directories(directories: Sequence[str | Path]) -> tuple[Path, ...]:
    resolved = []
    for directory in directories:
        try:
            resolved.append(Path(directory).resolve())
        except (OSError, RuntimeError):
            continue
    return tuple(resolved)


def _in_directories(path: str | bytes | Path, directories: tuple[Path, ...]) -> bool:
    "Tell whether a path, with '..' and symbolic links resolved, is in a directory."
    try:
        resolved = Path(os.fsdecode(path)).resolve()
    except (OSError, RuntimeError, ValueError):
        return False
    return any(resolved.is_relative_to(directory) for directory in directories)


def _file_state(path: str) -> FileState:
    """What identifies the current content of a file, or None where there is none.

    The change time is included because, unlike the modification time, it
    cannot be set to an earlier value.
    """
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return (stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def _files_unchanged(states: Sequence[tuple[str, FileState]]) -> bool:
    return all(_file_state(path) == state for path, state in states)


def _printf_name(name: str, block: int) -> str:
    "Fill in the block number in a printf-style name of a virtual dataset source."
    return name.replace("%%", "\0").replace("%b", str(block)).replace("\0", "%")


def _open_source(path: str, **kwargs: Any) -> h5py.File:
    "Open a source file of a virtual dataset, to look at what it refers to in turn."
    return h5py.File(path, mode="r", **kwargs)


class ReferenceGuard:
    """Refuse HDF5 objects whose data is, or may be, outside of given directories.

    An HDF5 file can make the library read other files: an external link names
    an object in another file, a virtual dataset maps onto datasets in other
    files, and a dataset can keep its raw data in any other file. A server that
    serves a file it did not write follows these only into the directories it
    is configured to read, and does not follow the last kind at all.

    Parameters
    ----------
    filename :
        The file that was opened.
    directories :
        The directories that may be read. None lifts every restriction, for
        use on one's own files.
    open_kwargs :
        Passed to h5py.File when a source of a virtual dataset is opened.
    """

    def __init__(
        self,
        filename: str | Path,
        directories: Sequence[str | Path] | None,
        open_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.filename = os.fspath(filename)
        self.restricted = directories is not None
        self._directories = directories
        self.open_kwargs = open_kwargs or {}

    @functools.cached_property
    def directories(self) -> tuple[Path, ...]:
        # Resolved only if a file refers to another one. Most do not.
        return _resolve_directories(self._directories or ())

    def check(self, obj: Any, follow_virtual: bool = True) -> None:
        """Raise BrokenLink unless the object may be read.

        With follow_virtual=False the sources of a virtual dataset are not
        looked at. That leaves the costly part for when the dataset is opened.
        """
        if not self.restricted or not isinstance(obj, (h5py.Group, h5py.Dataset)):
            return
        self._check(obj, self.filename, follow_virtual, depth=0, budget=[0])

    def _check(
        self,
        obj: h5py.Group | h5py.Dataset,
        opened: str,
        follow_virtual: bool,
        depth: int,
        budget: list[int],
        states: list[tuple[str, FileState]] | None = None,
    ) -> None:
        """Check one object.

        states, when given, belongs to the virtual dataset through which this
        object was reached. Every file that the result for this object depends
        on is added to it, so that the outer result is re-checked when any
        file below it changes.
        """
        # An external link was followed if the object is in another file than
        # the one that was opened. HDF5 reports the name of the file it found.
        home = os.fsdecode(obj.file.filename)
        if (
            home != opened
            and os.path.realpath(home) != os.path.realpath(opened)
            and not _in_directories(home, self.directories)
        ):
            raise BrokenLink(
                f"{obj.name} is reached through a link to a file outside of the "
                "storage that this server reads."
            )
        if not isinstance(obj, h5py.Dataset):
            return
        if obj.external:
            raise BrokenLink(
                f"The data of {obj.name} is stored in an external file, which "
                "this server does not read."
            )
        if obj.is_virtual and follow_virtual:
            key = (os.path.realpath(home), obj.name, self.directories)
            with _CHECKED_VIRTUAL_LOCK:
                checked = _CHECKED_VIRTUAL.get(key)
            if checked is None or not _files_unchanged(checked):
                checked = []
                self._check_virtual_sources(obj, checked, depth, budget)
                with _CHECKED_VIRTUAL_LOCK:
                    _CHECKED_VIRTUAL[key] = checked
            if states is not None:
                states.extend(checked)

    def _check_virtual_sources(
        self,
        dataset: h5py.Dataset,
        states: list[tuple[str, FileState]],
        depth: int,
        budget: list[int],
    ) -> None:
        "Check every source of a virtual dataset. Record the files looked at."
        if depth >= MAX_VIRTUAL_DEPTH:
            raise BrokenLink(
                f"The virtual dataset {dataset.name} is nested too deeply to check."
            )
        home = os.path.join(os.getcwd(), os.fsdecode(dataset.file.filename))
        states.append((home, _file_state(home)))
        for source in dataset.virtual_sources():
            printf_style = "%b" in source.file_name or "%b" in source.dset_name
            # A printf-style source stands for blocks 0, 1, 2, ... up to the
            # first one that does not exist.
            for block in itertools.count():
                budget[0] += 1
                if budget[0] > MAX_VIRTUAL_SOURCES:
                    raise BrokenLink(
                        f"The virtual dataset {dataset.name} has too many "
                        "sources to check."
                    )
                found = self._check_virtual_source(
                    dataset,
                    home,
                    _printf_name(source.file_name, block),
                    _printf_name(source.dset_name, block),
                    states,
                    depth,
                    budget,
                )
                if not (printf_style and found):
                    break

    def _check_virtual_source(
        self,
        dataset: h5py.Dataset,
        home: str,
        file_name: str,
        dataset_name: str,
        states: list[tuple[str, FileState]],
        depth: int,
        budget: list[int],
    ) -> bool:
        "Check one source dataset. Return False if it does not exist."
        if file_name == ".":
            # The source is in the same file as the virtual dataset.
            try:
                source = dataset.file[dataset_name]
            except KeyError:
                return False
            self._check(source, home, True, depth + 1, budget, states)
            return True
        # HDF5 looks for the source file in several places and reads the first
        # one it finds. Every place where a file exists must be acceptable.
        found = False
        for path in _virtual_source_candidates(home, file_name):
            # The result depends on this file, also on its being absent.
            state = _file_state(path)
            states.append((path, state))
            if state is None:
                continue
            found = True
            if not _in_directories(path, self.directories):
                raise BrokenLink(
                    f"The virtual dataset {dataset.name} has a source file "
                    "outside of the storage that this server reads."
                )
            try:
                file = _open_source(path, **self.open_kwargs)
            except OSError as err:
                raise BrokenLink(
                    f"A source file of the virtual dataset {dataset.name} cannot "
                    "be opened to check it."
                ) from err
            with file:
                try:
                    source = file[dataset_name]
                except KeyError:
                    continue
                self._check(source, path, True, depth + 1, budget, states)
        return found  # If False, HDF5 uses the fill value.


def _virtual_source_candidates(home: str, file_name: str) -> list[str]:
    """The paths where HDF5 may look for a source file of a virtual dataset.

    Given the name of the file that holds the virtual dataset, as it was
    opened, and the name of the source file as stored in it. HDF5 tries an
    absolute name as it is. Otherwise, and also if there is no file by the
    absolute name, it looks for the name, or the last part of the absolute
    name, under each prefix in HDF5_VDS_PREFIX, next to the file that holds
    the virtual dataset, in the working directory of the process, and next to
    the file that the name of the holding file leads to, if that name is a
    symbolic link.

    Paths are not normalized here: a `..` is resolved by the filesystem when
    a candidate is looked up, as HDF5 resolves it, so that a link on the way
    is followed the same.
    """
    cwd = os.getcwd()
    # Where HDF5 looks for a relative name: the directory part of the name of
    # the holding file, which it makes absolute without resolving anything.
    home_directory = os.path.dirname(os.path.join(cwd, home))
    candidates = []
    if os.path.isabs(file_name) or file_name[:1] in ("/", "\\"):
        # HDF5 counts a name that starts at a root as absolute, also on
        # Windows, where Python requires a drive as well.
        candidates.append(file_name)
        file_name = os.path.basename(file_name)
    for prefix in os.environ.get("HDF5_VDS_PREFIX", "").split(os.pathsep):
        if not prefix:
            continue
        if prefix.startswith("${ORIGIN}"):
            prefix = home_directory + prefix[len("${ORIGIN}") :]
        candidates.append(os.path.join(prefix, file_name))
    candidates.append(os.path.join(home_directory, file_name))
    candidates.append(os.path.join(cwd, file_name))
    candidates.append(os.path.join(os.path.dirname(os.path.realpath(home)), file_name))
    return list(dict.fromkeys(candidates))


def require_self_contained(filename: str | Path) -> None:
    """Raise BrokenLink if an HDF5 file refers to any other file.

    This is for files that another library reads, such as NetCDF-4 files,
    where each object cannot be checked as it is opened. Such files have no
    use for external links, virtual datasets or external raw storage. A file
    that is not in the HDF5 format is passed.
    """
    if not h5py.is_hdf5(filename):
        return

    def visit(name: str, obj: Any) -> str | None:
        if isinstance(obj, h5py.Dataset):
            if obj.external or obj.is_virtual:
                return str(obj.name)
            return None
        if not isinstance(obj, h5py.Group):
            return None  # A committed datatype refers to no file.
        for key in obj:
            if isinstance(obj.get(key, getlink=True), h5py.ExternalLink):
                return f"{obj.name.rstrip('/')}/{key}"
        return None

    with h5py.File(filename, mode="r") as file:
        # visititems does not call the function on the root group.
        found = visit("/", file) or file.visititems(visit)
    if found is not None:
        raise BrokenLink(
            f"{found} refers to another file, which this server does not read "
            "for this format."
        )


def parse_hdf5_tree(
    tree: h5py.File | h5py.Group | h5py.Dataset,
    check: Callable[..., None] | None = None,
) -> dict[str, Any | Sentinel] | Sentinel:
    """Parse an HDF5 file or group into a nested dictionary structure

    the resulting tree structure represents any groups as nested dictionaries ans datasets as None.

    Parameters
    ----------
    tree : h5py.File or h5py.Group
        The file or group to parse
    check : callable, optional
        Called with each object and follow_virtual=False. An object for which it
        raises BrokenLink is recorded as a broken link. See ReferenceGuard.

    Returns
    -------
    dict
        A nested dictionary structure representing the HDF5 file or group
    """
    res: dict[str, Any | None] = {}

    if isinstance(tree, h5py.Dataset):
        return HDF5_DATASET

    for key, val in tree.items():
        if val is not None and check is not None:
            try:
                check(val, follow_virtual=False)
            except BrokenLink:
                val = None
        res[key] = HDF5_BROKEN_LINK if val is None else parse_hdf5_tree(val, check)

    return res


def get_hdf5_attrs(
    file_uri: str,
    dataset: str | None = None,
    swmr: bool = SWMR_DEFAULT,
    libver: str = "latest",
    locking: bool | str | None = None,
    readable_directories: Sequence[str | Path] | None = None,
) -> JSON:
    """Get attributes of an HDF5 dataset"""
    d, _ = get_hdf5_attrs_and_soft_link(
        file_uri,
        dataset,
        swmr=swmr,
        libver=libver,
        locking=locking,
        readable_directories=readable_directories,
    )
    return d


def get_hdf5_attrs_and_soft_link(
    file_uri: str,
    dataset: str | None = None,
    swmr: bool = SWMR_DEFAULT,
    libver: str = "latest",
    locking: bool | str | None = None,
    readable_directories: Sequence[str | Path] | None = None,
) -> tuple[JSON, str | None]:
    """Get attributes of an HDF5 dataset and the target of the soft link it is reached through

    The target is returned relative to the group containing the link, so that it
    still resolves when the tree is written out to a different location, e.g. by
    an export. Soft links that are already relative are returned unchanged.
    """
    file_path = path_from_uri(file_uri)
    key = dataset.strip("/") if dataset else ""
    opened = h5open(
        file_path,
        swmr=swmr,
        libver=libver,
        locking=locking,
        readable_directories=readable_directories,
    )
    with opened as file:
        link = file.get(key, getlink=True) if key else None
        node = file[dataset] if dataset else file
        opened.guard.check(node)
        d = dict(getattr(node, "attrs", {}))
        for k, v in d.items():
            if isinstance(v, h5py.Empty):
                # An attribute with a null dataspace has no value.
                d[k] = None
            elif isinstance(v, bytes):
                # Convert any bytes to str. HDF5 strings are only tagged ASCII
                # or UTF-8, but older instrument software often wrote
                # Windows-1252 or Latin-1. Latin-1 decodes any bytes, so it
                # comes last.
                for encoding in ("utf-8", "cp1252", "latin-1"):
                    try:
                        d[k] = v.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        pass
    if not isinstance(link, h5py.SoftLink):
        return d, None
    if not link.path.startswith("/"):
        return d, link.path
    parent = posixpath.dirname("/" + key)
    return d, posixpath.relpath(link.path, parent)


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

    It also raises BrokenLink if the requested dataset does not exist in the file at all (e.g. a registered
    dataset was never written or has been removed), in which case h5py raises:
    KeyError: "Unable to synchronously open object (object 'y' doesn't exist)"

    If `readable_directories` is given, it raises BrokenLink as well when the object refers to data in a file
    outside of those directories, or keeps its raw data in an external file. See ReferenceGuard.
    """

    def __init__(
        self,
        filename: str | Path,
        dataset: str | None = None,
        readable_directories: Sequence[str | Path] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(filename, mode="r", **kwargs)
        self.dataset = dataset
        self.guard = ReferenceGuard(filename, readable_directories, kwargs)

    def __enter__(self) -> h5py.File | h5py.Group | h5py.Dataset:
        super().__enter__()
        try:
            obj = self[self.dataset] if self.dataset else self
            self.guard.check(obj)
            return obj
        except Exception:
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:  # type: ignore
        super().__exit__(exc_type, exc_value, exc_tb)

        if exc_type is KeyError:
            if "file" in str(exc_value):
                # External link is broken
                raise BrokenLink(exc_value.args[0]) from exc_value

            elif "component not found" in str(exc_value):
                # Soft link is broken
                raise BrokenLink(exc_value.args[0]) from exc_value

            elif "doesn't exist" in str(exc_value):
                # Registered dataset (or group) is missing from the file
                raise BrokenLink(exc_value.args[0]) from exc_value


class HDF5ArrayAdapter(ArrayAdapter):
    """Adapter for array-type data stored in HDF5 files

    This adapter lazily loads array data from HDF5 files using Dask. Supports reading from datasets spanning
    multiple files.

    Parameters
    ----------
    soft_link : str or bool, optional
        The target of the soft link this dataset is reached through, relative to the
        group that holds the link. False by default, so an export writes the data.
    """

    # The catalog passes the directories of its readable storage to from_catalog as `readable_directories`.
    accepts_readable_directories = True

    def __init__(
        self,
        array: NDArray[Any],
        structure: ArrayStructure,
        *,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
        soft_link: str | bool = False,
    ) -> None:
        self.soft_link = soft_link
        super().__init__(array, structure, metadata=metadata, specs=specs)

    @staticmethod
    def lazy_load_hdf5_array(
        *file_paths: str | Path,
        dataset: str | None = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: bool | str | None = None,
        readable_directories: Sequence[str | Path] | None = None,
    ) -> dask.array.Array:
        """Lazily load arrays from possibly multiple HDF5 files and concatenate them along the first axis

        The chunks of the resulting Dask array are determined by the chunks of the constituent arrays.

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
        readable_directories : list, optional
            If given, data that the files refer to in other files is read
            only from these directories. See ReferenceGuard.
        """
        open_kwargs: dict[str, Any] = dict(
            swmr=swmr,
            libver=libver,
            locking=locking,
            readable_directories=readable_directories,
        )

        # Define helper functions for reading and getting specs of HDF5 arrays
        def _read_hdf5_array(
            fpath: str | Path, slice: tuple[builtins.slice, ...]
        ) -> NDArray:
            with h5open(fpath, dataset, **open_kwargs) as ds:
                return ds[slice]

        def _get_hdf5_specs(
            fpath: str | Path,
        ) -> tuple[tuple[int, ...], tuple[int, ...], numpy.dtype]:
            with h5open(fpath, dataset, **open_kwargs) as ds:
                result = ds.shape, ds.chunks or ds.shape, ds.dtype
            return result

        # Need to know shapes/dtypes of constituent arrays to load them lazily
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

            def _read_as_bytes(fpath: str | Path) -> NDArray:
                with h5open(fpath, dataset, **open_kwargs) as ds:
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
            key_rest: tuple[tuple[int, ...], ...]
            slc_rest: tuple[tuple[builtins.slice, ...], ...]
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
        dataset: str | None = None,
        slice: str | NDSlice | None = None,
        squeeze: bool | None = False,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: bool | str | None = None,
        readable_directories: Sequence[str | Path] | None = None,
    ) -> "HDF5ArrayAdapter":
        structure = data_source.structure
        assets = data_source.assets
        data_uris = [
            ast.data_uri for ast in assets if ast.parameter == "data_uris"
        ] or [assets[0].data_uri]
        file_paths = [path_from_uri(uri) for uri in data_uris]

        array = cls.lazy_load_hdf5_array(
            *file_paths,
            dataset=dataset,
            swmr=swmr,
            libver=libver,
            locking=locking,
            readable_directories=readable_directories,
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
        attrs, soft_link = get_hdf5_attrs_and_soft_link(
            data_uris[0],
            dataset,
            swmr=swmr,
            libver=libver,
            locking=locking,
            readable_directories=readable_directories,
        )
        metadata = copy.deepcopy(node.metadata_)
        metadata.update(attrs)

        return cls(
            array,
            structure,
            metadata=metadata,
            specs=node.specs,
            soft_link=soft_link or False,
        )

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        dataset: str | None = None,
        slice: str | NDSlice | None = None,
        squeeze: bool = False,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: bool | str | None = None,
        readable_directories: Sequence[str | Path] | None = None,
    ) -> "HDF5ArrayAdapter":
        file_paths = [path_from_uri(uri) for uri in data_uris]

        array = cls.lazy_load_hdf5_array(
            *file_paths,
            dataset=dataset,
            swmr=swmr,
            libver=libver,
            locking=locking,
            readable_directories=readable_directories,
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
        attrs, soft_link = get_hdf5_attrs_and_soft_link(
            data_uris[0],
            dataset,
            swmr=swmr,
            libver=libver,
            locking=locking,
            readable_directories=readable_directories,
        )

        return cls(array, structure, metadata=attrs, soft_link=soft_link or False)


class HDF5Adapter(ContainerAdapter["HDF5Adapter | HDF5ArrayAdapter"], IndexersMixin):
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

    # The catalog passes the directories of its readable storage to from_catalog as `readable_directories`.
    accepts_readable_directories = True

    def __init__(
        self,
        tree: dict[str, Any] | Sentinel,
        *data_uris: str,
        dataset: str | None = None,
        structure: ArrayStructure | None = None,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
        **kwargs: Any | None,
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
        data_source: DataSource[ArrayStructure | None],
        node: Node,
        /,
        dataset: str | list[str] = "/",
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: bool | str | None = None,
        readable_directories: Sequence[str | Path] | None = None,
        **kwargs: Any,  # Optional kwargs for HDF5ArrayAdapter
    ) -> "HDF5Adapter | HDF5ArrayAdapter":
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
                readable_directories=readable_directories,
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
        opened = h5open(
            file_path,
            dataset,
            swmr=swmr,
            libver=libver,
            locking=locking,
            readable_directories=readable_directories,
        )
        with opened as file:
            tree = parse_hdf5_tree(file, opened.guard.check)

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
            readable_directories=readable_directories,
        )

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        dataset: str | None = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        locking: bool | str | None = None,
        readable_directories: Sequence[str | Path] | None = None,
        **kwargs: Any,  # Optional kwargs for HDF5ArrayAdapter
    ) -> "HDF5Adapter | HDF5ArrayAdapter":
        fpath = path_from_uri(data_uris[0])
        opened = h5open(
            fpath,
            dataset,
            swmr=swmr,
            libver=libver,
            locking=locking,
            readable_directories=readable_directories,
        )
        with opened as file:
            tree = parse_hdf5_tree(file, opened.guard.check)

        if tree == HDF5_DATASET:
            return HDF5ArrayAdapter.from_uris(
                *data_uris,
                dataset=dataset,
                swmr=swmr,
                libver=libver,
                locking=locking,
                readable_directories=readable_directories,
                **kwargs,
            )

        return cls(
            tree,
            *data_uris,
            dataset=dataset,
            swmr=swmr,
            libver=libver,
            locking=locking,
            readable_directories=readable_directories,
        )

    def __repr__(self) -> str:
        return node_repr(self, list(self))

    def metadata(self) -> JSON:
        d = get_hdf5_attrs(
            self.uris[0],
            self.dataset,
            readable_directories=self._kwargs.get("readable_directories"),
        )
        return {**d, **super().metadata()}

    def __iter__(self) -> Iterator[Any]:
        """Iterate over the keys of the tree"""
        yield from self._tree

    def __getitem__(self, key: str) -> "HDF5Adapter | HDF5ArrayAdapter":
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

    def get(self, key: str, *args: Any) -> "HDF5Adapter | HDF5ArrayAdapter":
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

    def read(self, fields: str | None = None) -> "HDF5Adapter":
        if fields is not None:
            raise NotImplementedError
        return self

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(
        self, start: int, stop: int, direction: int, page_size: int | None = None
    ) -> list[Any]:
        keys = list(self._tree.keys())
        if direction < 0:
            keys = list(reversed(keys))
        return keys[start:stop]

    def _items_slice(
        self, start: int, stop: int, direction: int, page_size: int | None = None
    ) -> list[tuple[Any, Any]]:
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
