from __future__ import annotations

import itertools
import sys
from typing import TYPE_CHECKING, Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import copy
import hashlib
import logging
import re
from collections.abc import Set
from contextlib import closing
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)
from urllib.parse import quote_plus

import awkward
import ragged

from tiled.adapters.core import Adapter
from tiled.adapters.resource_cache import with_resource_cache
from tiled.adapters.utils import init_adapter_from_catalog
from tiled.ndslice import NDBlock, NDSlice
from tiled.storage import FileStorage, Storage
from tiled.structures.data_source import Asset, DataSource
from tiled.structures.ragged import RaggedStructure
from tiled.utils import path_from_uri

if TYPE_CHECKING:
    from tiled.catalog.orm import Node
    from tiled.structures.core import Spec
    from tiled.type_aliases import JSON

import numpy
import pandas
import pyarrow
import ragged
from sqlalchemy.sql.compiler import RESERVED_WORDS

from tiled.ndslice import NDBlock, NDSlice
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.data_source import DataSource
from tiled.structures.ragged import RaggedStructure, make_ragged_array

from ..catalog.orm import Node
from ..storage import (
    EmbeddedSQLStorage,
    RemoteSQLStorage,
    SQLStorage,
    Storage,
    get_storage,
)
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource
from ..structures.table import TableStructure
from ..type_aliases import JSON
from .sql import SQLAdapter
from .utils import init_adapter_from_catalog

if TYPE_CHECKING:
    from collections.abc import Iterable

    import awkward
    from numpy.typing import NDArray

    from tiled.type_aliases import JSON


class RaggedAdapter(Adapter[RaggedStructure]):
    structure_family = StructureFamily.ragged

    def __init__(
        self,
        array: ragged.array | None,
        structure: RaggedStructure,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> None:
        """Create an adapter for the given ragged array and structure."""
        self._array = array
        self._structure = structure
        self._metadata = metadata or {}
        self.specs = list(specs or [])

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[RaggedStructure],
        node: Node,
        /,
        **kwargs: Any | None,
    ) -> Self:
        """Create a RaggedAdapter from a catalog entry."""
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)

    @classmethod
    def from_array(
        cls,
        array: ragged.array | awkward.Array | NDArray[Any] | Iterable[Iterable[Any]],
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> Self:
        """Create a RaggedAdapter wrapping a given array."""
        array = make_ragged_array(array)
        structure = RaggedStructure.from_array(array)
        return cls(
            array,
            structure,
            metadata=metadata,
            specs=specs,
        )

    def read(self, slice: NDSlice | None = None) -> ragged.array:
        """Read a slice of data from the ragged array."""
        if self._array is None:
            raise NotImplementedError
        return self._array[slice] if slice else self._array

    def read_block(self, block: NDBlock, slice: NDSlice | None = None) -> ragged.array:
        """Read a single partition block of the ragged array."""
        if self._array is None:
            raise NotImplementedError
        chunks = self._structure.chunks
        stops = list(itertools.accumulate(chunks))
        starts = [0] + stops[:-1]
        start = starts[block[0]]
        stop = stops[block[0]]
        data = self._array[start:stop]
        return data[slice] if slice else data

    def write(self, array: ragged.array) -> None:
        """Write the full ragged array (in-memory, replaces existing data)."""
        self._array = make_ragged_array(array)

    def write_block(self, array: ragged.array, block: NDBlock) -> None:
        """Write a single partition block (not supported for in-memory adapter)."""
        raise NotImplementedError(
            "write_block is not supported for the in-memory RaggedAdapter; "
            "use RaggedParquetAdapter for persistent block-wise writes."
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure})"


class RaggedSQLAdapter(Adapter[RaggedStructure]):
    structure_family = StructureFamily.ragged

    def __init__(
        self,
        tabular_adapter: SQLAdapter,
        structure: RaggedStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        self._tabular_adapter = tabular_adapter
        self._structure = structure
        self._metadata = metadata or {}
        self.specs = list(specs or [])

    @classmethod
    def supported_storage(cls) -> Set[type[Storage]]:
        return {SQLStorage, EmbeddedSQLStorage, RemoteSQLStorage}

    @classmethod
    def _get_tabular_data_source(
        cls, data_source: DataSource[RaggedStructure]
    ) -> DataSource[TableStructure]:
        "Convert a DataSource with a RaggedStructure to one with a TableStructure"
        awk_schema = data_source.structure.awk_form.expected_from_buffers()
        empty_table_dict = {col: [numpy.empty(0, dtype=typ)] for col, typ in awk_schema.items()}
        empty_table_dict["chunk_index"] = [0]

        return DataSource(
            structure_family=StructureFamily.table,
            structure=TableStructure.from_dict(empty_table_dict),
            parameters=data_source.parameters | {"order_by": [("chunk_index", "asc")]},
            properties=data_source.properties,
            mimetype="application/x-tiled-sql-table",
            assets=data_source.assets,
            management=data_source.management,
        )

    @classmethod
    def init_storage(
        cls,
        storage: SQLStorage,
        data_source: DataSource[RaggedStructure],
        path_parts: Optional[List[str]] = None,
    ) -> DataSource[RaggedStructure]:
        "Initialize an SQL storage for the data source."

        data_source = copy.deepcopy(data_source)  # Do not mutate caller input

        tabular_data_source = SQLAdapter.init_storage(
            storage=storage, data_source=cls._get_tabular_data_source(data_source)
        )

        data_source.assets.extend(tabular_data_source.assets)
        data_source.parameters["table_name"] = tabular_data_source.parameters["table_name"]
        data_source.parameters["dataset_id"] = tabular_data_source.parameters["dataset_id"]

        return data_source

    @classmethod
    def from_catalog(cls, data_source: DataSource[RaggedStructure], node: Node, **kwargs) -> Self:
        tabular_data_source = cls._get_tabular_data_source(data_source)
        tabular_adapter = SQLAdapter.from_catalog(tabular_data_source, node)

        return cls(
            tabular_adapter=tabular_adapter,
            structure=data_source.structure,
            metadata=node.metadata_,
            specs=node.specs,
        )

    def read(self, slice: NDSlice | None = None) -> ragged.array:
        "Read a slice of data from the ragged array."
        rows = self._tabular_adapter._read_full_table_or_partition().to_pylist()

        # Each row in the table represents an awkward container; concatenate them together
        form = self._structure.awk_form  # awkward form should be the same for each row
        containers = [{key: numpy.array(row[key], dtype=typ) for key, typ in form.expected_from_buffers().items()} for row in rows]
        lengths = [len(container["node0-offsets"]) - 1 for container in containers]
        awk_arrays = [awkward.from_buffers(form, l, c) for l, c in zip(lengths, containers)]

        array = ragged.array(awkward.concatenate(awk_arrays))

        return array[slice] if slice else array

    def read_block(self, block: NDBlock, slice: NDSlice | None = None) -> ragged.array:
        "Read a single block of chunks of the ragged array."

        # Slice the whole array to get this block and then slice within the block
        array = self.read(slice=block.slice_from_chunks(self._structure.chunks))

        return array[slice] if slice else array

    def write(self, data: ragged.array, slice: NDSlice = NDSlice(...)) -> None:
        if slice:
            raise NotImplementedError

        form, _, container = awkward.to_buffers(make_ragged_array(data)._impl)

        if self.structure().awk_form != form:
            raise ValueError("The structure of the provided data does not match the adapter")

        # Check if the SQL table is empty before writing, to prevent overwriting existing data
        if not self._tabular_adapter.read(fields=["chunk_index"]).empty:
            raise NotImplementedError("Overwriting of existing data is not supported")

        container["chunk_index"] = 0
        self._tabular_adapter.append_partition(0, pyarrow.Table.from_pylist([container]))

    def write_block(self, array: ragged.array, block: NDBlock) -> None:
        """Write a single partition block"""
        raise NotImplementedError('...')
    
    def append(self, data: ragged.array) -> RaggedStructure:
        raise NotImplementedError('...')

    def patch(self, data: ragged.array, offset: int, extend: bool = False) -> RaggedStructure:
        """Write data into a slice of the ragged array, possibly extending it.

        If the specified slice does not fit into the array, and extend=True, the
        array will be resized (expanded, never shrunk) to fit it.

        Parameters
        ----------
        data : ragged array-like
        offset : int
            Where to place the new data along the first dimension of the array.
        extend : bool
            If slice does not fit wholly within the shape of the existing array,
            reshape (expand) it to fit if this is True.

        Raises
        ------
        ValueError :
            If slice does not fit wholly with the shape of the existing array
            and extend is False
        """
        data = make_ragged_array(data)
        form, length, container = awkward.to_buffers(data._impl)

        if self.structure().awk_form != form:
            raise ValueError("The structure of the provided data does not match the adapter")
        
        container["chunk_index"] = len(self.structure().chunks[0])
        
        self._tabular_adapter.append_partition(0, pyarrow.Table.from_pylist([container]))

        # Update the structure to reflect the new data
        self._structure.shape = (self._structure.shape[0] + length, *self._structure.shape[1:])
        self._structure.chunks = (self._structure.chunks[0] + (length,), *self._structure.chunks[1:])
        self._structure.size += data.size

        return self._structure


    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure})"


class RaggedParquetAdapter(Adapter[RaggedStructure]):
    def __init__(
        self,
        data_uris: list[str],
        structure: RaggedStructure,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> None:
        """Create an adapter for a ragged array stored as a Parquet file."""
        super().__init__(structure, metadata=metadata, specs=specs)
        if isinstance(data_uris, str):
            data_uris = [data_uris]
        self._block_paths = [path_from_uri(uri) for uri in data_uris]

    @classmethod
    def supported_storage(cls) -> set[type[Storage]]:
        return {FileStorage}

    @classmethod
    def from_catalog(
        cls, data_source: DataSource[RaggedStructure], node: Node, /, **kwargs: Any
    ) -> RaggedParquetAdapter:
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)

    @classmethod
    def init_storage(
        cls,
        storage: Storage,
        data_source: DataSource[RaggedStructure],
        path_parts: list[str],
    ) -> DataSource[RaggedStructure]:
        """Initialize the storage directory for the data source."""
        data_source = copy.deepcopy(data_source)  # Do not mutate caller input.
        data_uri = storage.uri + "".join(
            f"/{quote_plus(segment)}" for segment in path_parts
        )
        directory = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        assets = [
            Asset(
                data_uri=f"{data_uri}/block-{i}.parquet",
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
            for i in range(data_source.structure.npartitions)
        ]
        data_source.assets.extend(assets)
        return data_source

    def _read_from_cache_or_file(self, path: Path) -> awkward.Array:
        cache_key = (awkward.from_parquet, path)
        return with_resource_cache(cache_key, awkward.from_parquet, path)

    def _read_multiple_from_cache_or_files(self, paths: list[Path]) -> awkward.Array:
        data_rows = []
        for path in paths:
            data_rows.extend(self._read_from_cache_or_file(path))
        return awkward.from_iter(data_rows)

    def read(self, slice: NDSlice | None = None) -> ragged.array:
        """Read ragged array data from storage."""
        if self._structure.npartitions == 1:
            data = self._read_from_cache_or_file(self._block_paths[0])
        else:
            data = self._read_multiple_from_cache_or_files(self._block_paths)
        sliced_data = data[tuple(slice)] if slice else data
        return ragged.array(
            sliced_data, dtype=self._structure.data_type.to_numpy_dtype()
        )

    def read_block(self, block: NDBlock, slice: NDSlice | None = None) -> ragged.array:
        """Read a single block of the ragged array from storage."""
        paths = self._block_paths[block[0]]
        if isinstance(paths, Path):
            paths = [paths]
        data = self._read_multiple_from_cache_or_files(paths)
        sliced_data = data[tuple(slice)] if slice else data
        return ragged.array(
            sliced_data, dtype=self._structure.data_type.to_numpy_dtype()
        )

    def write(self, array: ragged.array) -> None:
        """Write ragged array data to storage."""
        if self._structure.npartitions != 1:
            raise NotImplementedError
        uri = self._block_paths[0]
        _ = awkward.to_parquet(array._impl, uri)

    def write_block(self, array: ragged.array, block: NDBlock) -> None:
        """Write a single block of the ragged array to storage."""
        uri = self._block_paths[block[0]]
        _ = awkward.to_parquet(array._impl, uri)
