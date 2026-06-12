from __future__ import annotations

import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import copy
from collections.abc import Set
from contextlib import closing
from typing import Any

import adbc_driver_manager
import awkward
import numpy
import pyarrow
import ragged

from ..catalog.orm import Node
from ..ndslice import NDBlock, NDSlice, block_for_slice
from ..storage import EmbeddedSQLStorage, RemoteSQLStorage, SQLStorage, Storage
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..structures.ragged import (
    CanonicalRaggedArray,
    RaggedCompatibleType,
    RaggedStructure,
    make_ragged_array,
)
from ..structures.table import TableStructure
from ..type_aliases import JSON
from ..utils import Conflicts
from .core import Adapter
from .sql import SQLAdapter, is_unique_violation


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
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
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
        awkward_schema = data_source.structure.awkward_form.expected_from_buffers()
        empty_table_dict = {
            col: [numpy.empty(0, dtype=typ)] for col, typ in awkward_schema.items()
        }
        empty_table_dict["chunk_index"] = [numpy.int64(0)]

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
        path_parts: list[str] | None = None,
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
        cls, data_source: DataSource[RaggedStructure], node: Node, **kwargs: Any
    ) -> Self:
        tabular_data_source = cls._get_tabular_data_source(data_source)
        tabular_adapter = SQLAdapter.from_catalog(tabular_data_source, node)

        return cls(
            tabular_adapter=tabular_adapter,
            structure=data_source.structure,
            metadata=node.metadata_,
            specs=node.specs,
        )

    def _load_chunks(self, chunk_indexes: list[int]) -> list[dict[str, Any]]:
        """Fetch SQL rows for the given chunk_indexes, ordered by chunk_index ASC.

        Bypasses ``_read_full_table_or_partition`` so the chunk_index filter
        is pushed into the SQL ``WHERE`` clause instead of being applied in
        Python after a full scan.
        """
        if not chunk_indexes:
            return []
        tab = self._tabular_adapter
        schema = tab.structure().arrow_schema_decoded
        req_cols = list(schema.names)
        in_list = ", ".join(str(int(i)) for i in chunk_indexes)
        query = (
            "SELECT "
            + ", ".join(f'"{c.lower()}"' for c in req_cols)
            + f' FROM "{tab.table_name}" '
            f"WHERE _dataset_id={tab.dataset_id} "
            f"AND chunk_index IN ({in_list}) "
            f"ORDER BY chunk_index ASC"
        )
        with closing(tab.storage.connect()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetch_arrow_table()
            conn.commit()
        if lwr_upr_case_mapping := {c.lower(): c for c in req_cols if c != c.lower()}:
            data = data.rename_columns(lwr_upr_case_mapping)
        target_schema = pyarrow.schema(
            [schema.field(schema.get_field_index(name)) for name in req_cols]
        )
        rows: list[dict[str, Any]] = data.cast(target_schema).to_pylist()
        return rows

    def read(self, slice: NDSlice | None = None) -> CanonicalRaggedArray:
        """Read a slice of data from the ragged array.

        Only axis 0 is chunked, so ``tiled.ndslice.block_for_slice`` is invoked
        on ``(chunks[0],)`` with the first slice component to pick the
        ``chunk_index`` set to fetch from SQL; higher-axis components pass
        through to ``make_ragged_array`` unchanged.
        """
        chunks0 = self._structure.chunks[0] or ()
        slc = NDSlice(slice or ())
        chunk_indexes = list(range(len(chunks0)))

        if chunks0 and slc and NDSlice(slc[0]):
            try:
                block, adjusted = block_for_slice((chunks0,), NDSlice(slc[0]))
            except IndexError:
                pass  # out-of-bounds; let make_ragged_array raise a clear error
            else:
                b0 = block[0]
                selected = (
                    [b0]
                    if isinstance(b0, int)
                    else list(range(*b0.indices(len(chunks0))))
                )
                if selected:
                    chunk_indexes = selected
                    slc = NDSlice(adjusted[0], *slc[1:])

        rows = self._load_chunks(chunk_indexes)

        # Each row in the table represents an awkward buffer; concatenate them together
        form = self._structure.awkward_form  # form should be the same for each row
        buffers_schema = form.expected_from_buffers()
        buffers = [
            {
                key: numpy.array(row[key], dtype=typ)
                for key, typ in buffers_schema.items()
            }
            for row in rows
        ]
        selected_lengths = [chunks0[i] for i in chunk_indexes]
        awk_arrays = [
            awkward.from_buffers(form, length, buffer)
            for length, buffer in zip(selected_lengths, buffers)
        ]

        return make_ragged_array(awkward.concatenate(awk_arrays), slice=slc)

    def write(self, data: CanonicalRaggedArray) -> None:
        self.write_block(data, block=NDBlock(0))

    def write_block(self, data: CanonicalRaggedArray, block: NDBlock) -> None:
        form, _, buffers = awkward.to_buffers(data._impl)
        buffers = {key: val.ravel() for key, val in buffers.items()}

        if self.structure().awkward_form != form:
            raise ValueError(
                "The structure of the provided data does not match the adapter"
            )

        buffers["chunk_index"] = numpy.int64(block[0])
        try:
            self._tabular_adapter.append_partition(
                0,
                pyarrow.Table.from_pydict({k: [v] for k, v in buffers.items()}),
            )
        except adbc_driver_manager.Error as exc:
            if not is_unique_violation(exc):
                raise
            raise Conflicts(
                f"Cannot write chunk with chunk_index={int(block[0])}: "
                "a chunk with this index already exists. This typically "
                "indicates a concurrent write to the same dataset; "
                "ragged arrays are designed for a single producer per dataset."
            ) from exc

    def patch(
        self, data: CanonicalRaggedArray, offset: tuple[int, ...], extend: bool = False
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

        if not offset:
            raise ValueError("`offset` must contain at least one dimension")

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

        if self.structure().awkward_form != form:
            raise ValueError(
                "The structure (AwkwardForm) of the data does not match the adapter"
            )

        buffers["chunk_index"] = numpy.int64(len(self.structure().chunks[0] or ()))

        try:
            self._tabular_adapter.append_partition(
                0,
                pyarrow.Table.from_pydict({k: [v] for k, v in buffers.items()}),
            )
        except adbc_driver_manager.Error as exc:
            if not is_unique_violation(exc):
                raise
            raise Conflicts(
                f"Cannot append chunk with chunk_index={int(buffers['chunk_index'])}: "
                "a chunk with this index already exists. This typically "
                "indicates a concurrent write to the same dataset; "
                "ragged arrays are designed for a single producer per dataset."
            ) from exc

        # Update the structure to reflect the new data
        self._structure.shape = (
            self._structure.shape[0] + length,
            *self._structure.shape[1:],
        )
        self._structure.chunks = (
            (self._structure.chunks[0] or ()) + (length,),
            *self._structure.chunks[1:],
        )
        self._structure.size += data.size

        return self._structure

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure})"
