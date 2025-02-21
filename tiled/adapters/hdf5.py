import builtins
import collections.abc
import os
import re
import sys
import warnings
from pathlib import Path
from types import EllipsisType
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
from ..type_aliases import JSON
from ..utils import node_repr, path_from_uri
from .array import ArrayAdapter
from .resource_cache import with_resource_cache

SWMR_DEFAULT = bool(int(os.getenv("TILED_HDF5_SWMR_DEFAULT", "0")))
INLINED_DEPTH = int(os.getenv("TILED_HDF5_INLINED_CONTENTS_MAX_DEPTH", "7"))


def from_dataset(dataset: NDArray[Any]) -> ArrayAdapter:
    return ArrayAdapter.from_array(dataset, metadata=getattr(dataset, "attrs", {}))


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


if sys.version_info < (3, 9):
    from typing_extensions import Mapping

    MappingType = Mapping
else:
    import collections

    MappingType = collections.abc.Mapping


def parse_hdf5_tree(arg: Union[h5py.File, h5py.Group]) -> dict[str, Any]:
    """Parse an HDF5 file or group into a nested dictionary structure

    the resulting tree structure represenets any groups as nested dictionaries ans datasets as None.
    
    Parameters
    ----------
    arg : h5py.File or h5py.Group
        The file or group to parse

    Returns
    -------
    dict
        A nested dictionary structure representing the HDF5 file or group
    """
    res = {}
    for key, val in arg.items():
        if isinstance(val, h5py.Group):
            res[key] = parse_hdf5_file(val)
        elif isinstance(val, h5py.Dataset):
            res[key] = None
    return res


class HDF5Adapter(MappingType[str, Union["HDF5Adapter", HDF5ArrayAdapter]], IndexersMixin):
    """
    Read an HDF5 file or a group within one.

    This map the structure of an HDF5 file onto a "Tree" of array structures.

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
        **kwargs: Optional[Any]
    ) -> None:
        self._tree = tree
        self.uris = data_uris
        self.dataset = dataset
        self.specs = specs or []
        self.swmr = swmr
        self.libver
        self._metadata = metadata or {}
        self._kwargs = kwargs   # e.g. swmr, libver, etc.

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
        if data_source.structure_family == StructureFamily.array:
            return HDF5ArrayAdapter.from_catalog(
                data_source, node, dataset=dataset, swmr=swmr, libver=libver, **kwargs
            )

        # Initialize adapter for the entire HDF5 tree
        assets = data_source.assets
        if len(assets) == 1:
            data_uris = [assets[0].data_uri]
        else:
            # for ast in assets:
            #     if ast.parameter == "data_uri":
            #         data_uri = ast.data_uri
            #         break
            data_uris = [ast.data_uri for ast in assets if ast.parameter == "data_uri"]
        filepath = path_from_uri(data_uri)
        cache_key = (h5py.File, filepath, "r", swmr, libver)
        file = with_resource_cache(
            cache_key, h5py.File, filepath, "r", swmr=swmr, libver=libver
        )

        adapter = cls(
            file,
            structure=data_source.structure,
            metadata=node.metadata_,
            specs=node.specs,
        )
        dataset = kwargs.get("dataset") or kwargs.get("path") or []
        if isinstance(dataset, str):
            dataset = dataset.strip("/").split('/')
        for segment in dataset:
            adapter = adapter.get(segment)  # type: ignore
            if adapter is None:
                raise KeyError(segment)

        return adapter

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        dataset: Optional[str] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
    ) -> "HDF5Adapter":
        fpath = path_from_uri(data_uri)
        cache_key = (h5py.File, fpath, "r", swmr, libver)
        file = with_resource_cache(
            cache_key, h5py.File, fpath, "r", swmr=swmr, libver=libver
        )
        return cls(file)

    def __repr__(self) -> str:
        return node_repr(self, list(self))

    def structure(self) -> None:
        return None

    def metadata(self) -> JSON:
        fpath = path_from_uri(self.uris[0])
        with h5py.File(fpath, "r") as _file:
            node = _file[self.dataset] if self.dataset else _file
            d = dict(getattr(node, "attrs", {}))
            for k, v in list(d.items()):
                # Convert any bytes to str.
                if isinstance(v, bytes):
                    d[k] = v.decode()
            d.update(self._metadata)
        return d

    def __iter__(self) -> Iterator[Any]:
        yield from self._tree    # Iterate over the keys of the tree

    def __getitem__(self, key: str) -> Union["HDF5Adapter", HDF5ArrayAdapter]:
        node = self._tree[key]
        dataset = f"{self.dataset}/{key}"   # Referenced to root of the file
        if isinstance(node, dict):
            return HDF5Adapter(node, *self.uris, dataset=dataset,
                               metadata=self._metadata, specs=self.specs, **self._kwargs)
        else:
            return HDF5ArrayAdapter.from_uris(*self.uris, dataset=dataset, **self._kwargs)

    def __len__(self) -> int:
        return len(self._tree)

    def keys(self) -> KeysView:  # type: ignore
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self) -> ValuesView:  # type: ignore
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self) -> ItemsView:  # type: ignore
        return ItemsView(lambda: len(self), self._items_slice)

    def search(self, query: Any) -> None:
        """

        Parameters
        ----------
        query :

        Returns
        -------
                Return a Tree with a subset of the mapping.

        """
        raise NotImplementedError

    def read(self, fields: Optional[str] = None) -> "HDF5Adapter":
        """

        Parameters
        ----------
        fields :

        Returns
        -------

        """
        if fields is not None:
            raise NotImplementedError
        return self

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(self, start: int, stop: int, direction: int) -> List[Any]:
        """

        Parameters
        ----------
        start :
        stop :
        direction :

        Returns
        -------

        """
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


class HDF5ArrayAdapter(ArrayAdapter):
    """Adapter for array-type data stored in HDF5 files"""

    @staticmethod
    def lazy_load_hdf5_array(
        *file_paths: Union[str, Path],
        dataset: Optional[str] = None,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
    ) -> dask.array.Array:
        """Lazily load arrays from possibly multiple HDF5 files"""

        def _read_hdf5_array(fpath: Union[str, Path]) -> NDArray[Any]:
            f = h5py.File(fpath, "r", swmr=swmr, libver=libver)
            return f[dataset] if dataset else f

        def _get_hdf5_specs(
            fpath: Union[str, Path]
        ) -> Tuple[Tuple[int, ...], numpy.dtype]:
            with h5py.File(fpath, "r", swmr=swmr, libver=libver) as f:
                f = f[dataset] if dataset else f
                return f.shape, f.chunks, f.dtype

        # Need to know shapes/dtypes of constituent arrays to load them lazily
        (shapes, chunks, dtypes) = [_get_hdf5_specs(fpath) for fpath in file_paths]
        if dtypes[0] == numpy.dtype("O"):
            assert len(file_paths) == 1, "Cannot handle object arrays from multiple files"
            warnings.warn(
                f"The dataset {key} is of object type, using a "
                "Python-only feature of h5py that is not supported by "
                "HDF5 in general. Read more about that feature at "
                "https://docs.h5py.org/en/stable/special.html. "
                "Consider using a fixed-length field instead. "
                "Tiled will serve an empty placeholder, unless the "
                "object is of size 1, where it will attempt to repackage "
                "the data into a numpy array."
            )
            # TODO: It should be possible to put this in dask.delayed too -- needs to be thoroughly tested

            check_str_dtype = h5py.check_string_dtype(dtypes[0])
            if check_str_dtype.length is None:
                with h5py.File(file_paths[0], "r", swmr=swmr, libver=libver) as f:
                    value = f[dataset] if dataset else f
                    dataset_names = value.file[f.name + "/" + key][...][()]
                    if value.size == 1:
                        arr = numpy.array(dataset_names)
                    # TODO: refactor and test
                return dask.array.from_array(arr)
            return dask.array.empty(shape=())
        
        delayed = [dask.delayed(_read_hdf5_array)(fpath) for fpath in file_paths]
        arrs = [
            dask.array.from_delayed(val, shape=shape, dtype=dtype).rechunk(chunk_shape)
            for (val, shape, chunk_shape, dtype) in zip(delayed, shapes, chunks, dtypes)
        ]
        array = dask.array.concatenate(arrs, axis=0)

        return array

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource,
        node: Node,
        /,
        dataset: Optional[str] = None,
        slice: Optional[str | Tuple[Union[int, builtins.slice, EllipsisType], ...]] = None,
        squeeze: bool = False,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        **kwargs: Optional[Any],
    ) -> "HDF5ArrayAdapter":
        """Adapter for array data stored in HDF5 files

        Parameters
        ----------
        data_source :
        node :
        kwargs : dict
        """

        structure = data_source.structure
        file_paths = [path_from_uri(ast.data_uri) for ast in data_source.assets]

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

        return cls(
            array,
            structure,
            metadata=node.metadata_,
            specs=node.specs,
        )

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        dataset: Optional[str] = None,
        slice: Optional[str | Tuple[Union[int, builtins.slice, EllipsisType], ...]] = None,
        squeeze: bool = False,
        swmr: bool = SWMR_DEFAULT,
        libver: str = "latest",
        **kwargs: Optional[Any],
    ) -> "HDF5ArrayAdapter":
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

        structure = ArrayStructure.from_array(array)

        return cls(array, structure)
