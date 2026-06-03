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
import numpy
import pandas
import pyarrow
import ragged
from sqlalchemy.sql.compiler import RESERVED_WORDS

from ..catalog.orm import Node
from ..ndslice import NDBlock, NDSlice
from ..storage import (
    EmbeddedSQLStorage,
    FileStorage,
    RemoteSQLStorage,
    SQLStorage,
    Storage,
    get_storage,
)
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource
from ..structures.ragged import (
    CanonicalRaggedArray,
    RaggedCompatibleType,
    RaggedSlicingError,
    RaggedStructure,
    make_ragged_array,
)
from ..structures.table import TableStructure
from ..type_aliases import JSON
from ..utils import path_from_uri
from .core import Adapter
from .resource_cache import with_resource_cache
from .sql import SQLAdapter
from .utils import init_adapter_from_catalog

if TYPE_CHECKING:
    from collections.abc import Iterable

    import awkward
    from numpy.typing import NDArray

    from tiled.type_aliases import JSON


class RaggedAdapter(Adapter[RaggedStructure]):
    """In-memory adapter for ragged arrays."""

    structure_family = StructureFamily.ragged

    def __init__(
        self,
        array: ragged.array | None,
        structure: RaggedStructure,
        *,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> None:
        self._array = array
        self._structure = structure
        self._metadata = metadata or {}
        self.specs = list(specs or [])

    @classmethod
    def from_array(
        cls,
        array: RaggedCompatibleType,
        *,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> Self:
        """Create a RaggedAdapter wrapping a given array."""
        return cls(
            array=make_ragged_array(array),
            structure=RaggedStructure.from_array(array),
            metadata=metadata,
            specs=specs,
        )

    def read(self, slice: NDSlice | None = None) -> ragged.array:
        """Read a slice of data from the ragged array, or the entire array."""
        return make_ragged_array(self._array, slice=slice)

    def read_block(self, block: NDBlock, slice: NDSlice | None = None) -> ragged.array:
        """Read a single partition block of the ragged array, possibly sliced."""

        data = make_ragged_array(
            self._array, slice=block.slice_from_chunks(self._structure.chunks)
        )

        return make_ragged_array(data, slice=slice)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure})"


class RaggedSQLAdapter(Adapter[RaggedStructure]):
    """Adapter for ragged arrays stored in SQL databases."""

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
        empty_table_dict = {
            col: [numpy.empty(0, dtype=typ)] for col, typ in awk_schema.items()
        }
        empty_table_dict["chunk_index"] = [0]

        return DataSource(
            structure_family=StructureFamily.table,
            structure=TableStructure.from_dict(empty_table_dict),
            parameters=data_source.parameters
            | {
                "order_by_args": [{"column": "chunk_index", "direction": "asc"}],
                "primary_key": ["chunk_index"],
            },
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
        data_source.parameters["table_name"] = tabular_data_source.parameters[
            "table_name"
        ]
        data_source.parameters["dataset_id"] = tabular_data_source.parameters[
            "dataset_id"
        ]

        return data_source

    @classmethod
    def from_catalog(
        cls, data_source: DataSource[RaggedStructure], node: Node, **kwargs
    ) -> Self:
        tabular_data_source = cls._get_tabular_data_source(data_source)
        tabular_adapter = SQLAdapter.from_catalog(tabular_data_source, node)

        return cls(
            tabular_adapter=tabular_adapter,
            structure=data_source.structure,
            metadata=node.metadata_,
            specs=node.specs,
        )

    def read(self, slice: NDSlice | None = None) -> CanonicalRaggedArray:
        "Read a slice of data from the ragged array."
        rows = self._tabular_adapter._read_full_table_or_partition().to_pylist()

        # Each row in the table represents an awkward buffer; concatenate them together
        form = self._structure.awk_form  # awkward form should be the same for each row
        buffers = [
            {
                key: numpy.array(row[key], dtype=typ)
                for key, typ in form.expected_from_buffers().items()
            }
            for row in rows
        ]
        awk_arrays = [
            awkward.from_buffers(form, l, b)
            for l, b in zip(self._structure.chunks[0], buffers)
        ]

        return make_ragged_array(awkward.concatenate(awk_arrays), slice=slice)

    def read_block(
        self, block: NDBlock, slice: NDSlice | None = None
    ) -> CanonicalRaggedArray:
        """Read a single block of chunks of the ragged array.

        Slices the whole array to get this block and then slices within the block
        """

        array = self.read(slice=block.slice_from_chunks(self._structure.chunks))

        return make_ragged_array(array, slice=slice)

    def write(self, data: CanonicalRaggedArray) -> None:
        self.write_block(data, block=NDBlock(0))

    def write_block(self, data: CanonicalRaggedArray, block: NDBlock) -> None:
        form, _, buffers = awkward.to_buffers(data._impl)
        buffers = {key: val.ravel() for key, val in buffers.items()}

        if self.structure().awk_form != form:
            raise ValueError(
                "The structure of the provided data does not match the adapter"
            )

        buffers["chunk_index"] = block[0]
        self._tabular_adapter.append_partition(0, pyarrow.Table.from_pylist([buffers]))

    def patch(
        self, data: CanonicalRaggedArray, offset: Tuple[int, ...], extend: bool = False
    ) -> RaggedStructure:
        """Write data into a slice of the ragged array, possibly extending it.

        If the specified slice does not fit into the array, and extend=True, the
        array will be resized (expanded, never shrunk) to fit it.

        Parameters
        ----------
        data : CanonicalRaggedArray
            The data to write into the array.
        offset : tuple[int, ...]
            Where to place the new data along the leftmost fixed dimensions of the array.
        extend : bool
            If slice does not fit wholly within the shape of the existing array,
            reshape (expand) it to fit if this is True.
        """
        if not extend:
            raise NotImplementedError("Overwriting existing data is not supported")

        ndim_fixed = self.structure().ndim_fixed
        if (
            (offset[0] != self.structure().shape[0])
            or any(offset[1:])
            or (len(offset) > ndim_fixed)
        ):
            raise NotImplementedError(
                "Only appending along the leftmost dimension is supported"
            )

        if data.shape[1:ndim_fixed] != self.structure().shape[1:ndim_fixed]:
            raise ValueError(
                "The shape of the data does not match the existing array along the fixed dimensions"
            )

        form, length, buffers = awkward.to_buffers(data._impl)
        buffers = {key: val.ravel() for key, val in buffers.items()}

        if self.structure().awk_form != form:
            raise ValueError(
                "The structure (AwkwardForm) of the data does not match the adapter"
            )

        buffers["chunk_index"] = len(self.structure().chunks[0])

        self._tabular_adapter.append_partition(0, pyarrow.Table.from_pylist([buffers]))

        # Update the structure to reflect the new data
        self._structure.shape = (
            self._structure.shape[0] + length,
            *self._structure.shape[1:],
        )
        self._structure.chunks = (
            self._structure.chunks[0] + (length,),
            *self._structure.chunks[1:],
        )
        self._structure.size += data.size

        return self._structure

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure})"
