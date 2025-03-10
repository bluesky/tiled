import builtins
import copy
import os
import re
import warnings
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterator, List, Optional, Tuple, Union

import dask
import dask.array
import dask.delayed
import h5py
import numpy
from numpy._typing import NDArray

from ..adapters.utils import IndexersMixin
from ..catalog.orm import Node
from ..iterviews import ItemsView, KeysView, ValuesView
from ..structures.array import ArrayStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON, EllipsisType
from ..utils import node_repr, path_from_uri
from .array import ArrayAdapter

SWMR_DEFAULT = bool(int(os.getenv("TILED_HDF5_SWMR_DEFAULT", "0")))
INLINED_DEPTH = int(os.getenv("TILED_HDF5_INLINED_CONTENTS_MAX_DEPTH", "7"))


def ndslice_from_string(
    arg: str,
) -> Tuple[Union[int, builtins.slice, EllipsisType], ...]:
    """Parse and convert a string representation of a slice

    For example, '(1:3, 4, 1:5:2, ...)' is converted to (slice(1, 3), 4, slice(1, 5, 2), ...).
    """
    if not (arg.startswith("[") and arg.endswith("]")) and not (
        arg.startswith("(") and arg.endswith(")")
    ):
        raise ValueError("Slice must be enclosed in square brackets or parentheses.")
    result: list[Union[int, builtins.slice, EllipsisType]] = []
    for part in arg[1:-1].split(","):
        if part.strip() == ":":
            result.append(builtins.slice(None))
        elif m := re.match(r"^(\d+):$", part.strip()):
            start, stop = int(m.group(1)), None
            result.append(builtins.slice(start, stop))
        elif m := re.match(r"^:(\d+)$", part.strip()):
            start, stop = 0, int(m.group(1))
            result.append(builtins.slice(start, stop))
        elif m := re.match(r"^(\d+):(\d+):(\d+)$", part.strip()):
            start, stop, step = map(int, m.groups())
            result.append(builtins.slice(start, stop, step))
        elif m := re.match(r"^(\d+):(\d+)$", part.strip()):
            start, stop = map(int, m.groups())
            result.append(builtins.slice(start, stop))
        elif m := re.match(r"^(\d+)$", part.strip()):
            result.append(int(m.group()))
        elif part.strip() == "...":
            result.append(Ellipsis)
        else:
            raise ValueError(f"Invalid slice part: {part}")
        # TODO: cases like "::n" or ":4:"
    return tuple(result)


def parse_hdf5_tree(
    tree: Union[h5py.File, h5py.Group, h5py.Dataset]
) -> Union[dict[str, Union[Any, None]], None]:
    """Parse an HDF5 file or group into a nested dictionary structure

    the resulting tree structure represenets any groups as nested dictionaries ans datasets as None.

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
        return None

    for key, val in tree.items():
        res[key] = parse_hdf5_tree(val)

    return res


def get_hdf5_attrs(
    file_uri: str,
    dataset: Optional[str] = None,
    swmr: bool = SWMR_DEFAULT,
    libver: str = "latest",
    **kwargs: Optional[Any],
) -> JSON:
    """Get attributes of an HDF5 dataset"""
    file_path = path_from_uri(file_uri)
    with h5py.File(file_path, "r", swmr=swmr, libver=libver, **kwargs) as _file:
        node = _file[dataset] if dataset else _file
        d = dict(getattr(node, "attrs", {}))
        for k, v in list(d.items()):
            # Convert any bytes to str.
            if isinstance(v, bytes):
                d[k] = v.decode()
    return d


class HDF5ArrayAdapter(ArrayAdapter):
    """Adapter for array-type data stored in HDF5 files

    This adapter lazily loads array data from HDF5 files using Dask. Supports reading from datasets spanning
    multiple files.
    """

    @staticmethod
    def lazy_load_hdf5_array(
        *file_paths: Union[str, Path],
        dataset: Optional[str] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
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
        """

        # Define helper functions for reading and getting specs of HDF5 arrays with dask.delayed
        def _read_hdf5_array(fpath: Union[str, Path]) -> NDArray[Any]:
            f = h5py.File(fpath, "r", swmr=swmr, libver=libver)
            return f[dataset][()] if dataset else f[()]

        def _get_hdf5_specs(
            fpath: Union[str, Path]
        ) -> Tuple[Tuple[int, ...], Union[Tuple[int, ...], None], numpy.dtype]:
            with h5py.File(fpath, "r", swmr=swmr, libver=libver) as f:
                f = f[dataset] if dataset else f
                return f.shape, f.chunks, f.dtype

        # Need to know shapes/dtypes of constituent arrays to load them lazily
        shapes_chunks_dtypes = [_get_hdf5_specs(fpath) for fpath in file_paths]
        dtype = shapes_chunks_dtypes[0][2]
        if dtype == numpy.dtype("O"):
            # TODO: It should be possible to put this in dask.delayed too -- needs to be thoroughly tested
            assert (
                len(file_paths) == 1
            ), "Cannot handle object arrays from multiple files"
            warnings.warn(
                f"The dataset {dataset} is of object type, using a "
                "Python-only feature of h5py that is not supported by "
                "HDF5 in general. Read more about that feature at "
                "https://docs.h5py.org/en/stable/special.html. "
                "Consider using a fixed-length field instead. "
                "Tiled will serve an empty placeholder, unless the "
                "object is of size 1, where it will attempt to repackage "
                "the data into a numpy array."
            )

            check_str_dtype = h5py.check_string_dtype(dtype)
            if check_str_dtype.length is None:
                # TODO: refactor and test
                with h5py.File(file_paths[0], "r", swmr=swmr, libver=libver) as f:
                    value = f[dataset] if dataset else f
                    dataset_names = value.file[f.name + "/" + dataset][...][()]
                    if value.size == 1:
                        arr = dask.array.from_array(numpy.array(dataset_names))
                    else:
                        arr = dask.array.empty(shape=())
                return arr
            return dask.array.empty(shape=())

        delayed = [dask.delayed(_read_hdf5_array)(fpath) for fpath in file_paths]
        arrs = [
            dask.array.from_delayed(val, shape=shape, dtype=dtype).rechunk(
                chunks=chunk_shape or "auto"
            )
            for (val, (shape, chunk_shape, dtype)) in zip(delayed, shapes_chunks_dtypes)
        ]
        array = dask.array.concatenate(arrs, axis=0)

        return array

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[ArrayStructure],
        node: Node,
        /,
        dataset: Optional[str] = None,
        slice: Optional[
            str | Tuple[Union[int, builtins.slice, EllipsisType], ...]
        ] = None,
        squeeze: Optional[bool] = False,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        **kwargs: Optional[Any],
    ) -> "HDF5ArrayAdapter":
        structure = data_source.structure
        assets = data_source.assets
        data_uris = [ast.data_uri for ast in assets if ast.parameter == "data_uri"] or [
            assets[0].data_uri
        ]
        file_paths = [path_from_uri(uri) for uri in data_uris]

        array = cls.lazy_load_hdf5_array(
            *file_paths, dataset=dataset, swmr=swmr, libver=libver
        )

        if slice:
            if isinstance(slice, str):
                slice = ndslice_from_string(slice)
            array = array[slice]
        if squeeze:
            array = array.squeeze()

        if array.shape != tuple(structure.shape):
            raise ValueError(
                f"Shape mismatch between array data and structure: "
                f"{array.shape} != {tuple(structure.shape)}"
            )
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
            get_hdf5_attrs(data_uris[0], dataset, swmr=swmr, libver=libver, **kwargs)
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
        slice: Optional[
            str | Tuple[Union[int, builtins.slice, EllipsisType], ...]
        ] = None,
        squeeze: bool = False,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        **kwargs: Optional[Any],
    ) -> "HDF5ArrayAdapter":
        file_paths = [path_from_uri(uri) for uri in data_uris]

        array = cls.lazy_load_hdf5_array(
            *file_paths, dataset=dataset, swmr=swmr, libver=libver
        )

        # Apply slice and squeeze operations, if specified
        if slice:
            if isinstance(slice, str):
                slice = ndslice_from_string(slice)
            array = array[slice]
        if squeeze:
            array = array.squeeze()

        # Construct the structure and pull additional metadata from the file attributes
        structure = ArrayStructure.from_array(array)
        metadata = get_hdf5_attrs(
            data_uris[0], dataset, swmr=swmr, libver=libver, **kwargs
        )

        return cls(array, structure, metadata=metadata)


class HDF5Adapter(Mapping[str, Union["HDF5Adapter", HDF5ArrayAdapter]], IndexersMixin):
    """Adapter for HDF5 files

    This map the structure of an HDF5 file onto a "Tree" of array structures.

    Parameters
    ----------
    tree : dict
        A dictionary representing the HDF5 file or group. The keys are the names of the groups or datasets,
        and the values are either dictionaries (representing groups) or None (representing datasets).
        HDF5 datasets will be mapped to HDF5ArrayAdapter instances, and groups will be mapped to HDF5Adapter
        instances.
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

    structure_family = StructureFamily.container

    def __init__(
        self,
        tree: dict[str, Any],
        *data_uris: str,
        dataset: Optional[str] = None,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        **kwargs: Optional[Any],
    ) -> None:
        # Traverse the tree to the desired HDF5 group
        self._tree = tree
        self.uris = data_uris
        self.dataset = dataset
        self.specs = specs or []
        self._metadata = metadata or {}
        self._kwargs = kwargs  # e.g. swmr, libver, etc.

    @classmethod
    def from_catalog(
        cls,
        # An HDF5 node may reference a dataset (array) or group (container).
        data_source: DataSource[Union[ArrayStructure, None]],
        node: Node,
        /,
        dataset: Optional[Union[str, list[str]]] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        **kwargs: Optional[Any],
    ) -> Union["HDF5Adapter", HDF5ArrayAdapter]:
        # Convert the dataset representation (for backward compatibility)
        dataset = dataset or kwargs.get("path") or []
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
                squeeze=False,
                **kwargs,
            )

        # Initialize adapter for the entire HDF5 tree
        # If multiple data_uri assests are supplied, only the first one is traversed, but the rest of the uris
        # will be kept in case we need to read an array that spans all of them.
        assets = data_source.assets
        assert len(assets) > 0, "No assets found in data source"
        data_uris = [ast.data_uri for ast in assets if ast.parameter == "data_uri"] or [
            assets[0].data_uri
        ]
        file_path = path_from_uri(data_uris[0])
        with h5py.File(file_path, "r", swmr=swmr, libver=libver) as file:
            tree = parse_hdf5_tree(file[dataset] if dataset else file)

        if tree is None:
            raise ValueError("Data source pointing to an HDF5 Dataset should have an array structure")

        return cls(
            tree,
            *data_uris,
            dataset=dataset,
            structure=data_source.structure,
            metadata=node.metadata_,
            specs=node.specs,
            swmr=swmr,
            libver=libver,
            **kwargs,
        )

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        dataset: Optional[str] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
    ) -> Union["HDF5Adapter", HDF5ArrayAdapter]:
        fpath = path_from_uri(data_uris[0])
        with h5py.File(fpath, "r", swmr=swmr, libver=libver) as file:
            tree = parse_hdf5_tree(file[dataset] if dataset else file)

        if tree is None:
            return HDF5ArrayAdapter.from_uris(
                *data_uris, dataset=dataset, swmr=swmr, libver=libver
            )

        return cls(tree, *data_uris, dataset=dataset, swmr=swmr, libver=libver)

    def __repr__(self) -> str:
        return node_repr(self, list(self))

    def structure(self) -> None:
        return None

    def metadata(self) -> JSON:
        d = get_hdf5_attrs(self.uris[0], self.dataset)
        d.update(self._metadata)
        return d

    def __iter__(self) -> Iterator[Any]:
        yield from self._tree  # Iterate over the keys of the tree

    def __getitem__(self, key: str) -> "HDF5Adapter":
        node = copy.deepcopy(self._tree)
        for segment in key.strip("/").split("/"):
            node = node[segment]
        dataset = f"{self.dataset or ''}/{key.strip('/')}"  # Referenced to the root of the file
        if isinstance(node, dict):
            return HDF5Adapter(
                node,
                *self.uris,
                dataset=dataset,
                metadata=self._metadata,
                specs=self.specs,
                **self._kwargs,
            )
        else:
            return HDF5ArrayAdapter.from_uris(
                *self.uris, dataset=dataset, **self._kwargs
            )

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

    def _keys_slice(self, start: int, stop: int, direction: int) -> List[Any]:
        keys = list(self._tree.keys())
        if direction < 0:
            keys = list(reversed(keys))
        return keys[start:stop]

    def _items_slice(
        self, start: int, stop: int, direction: int
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
