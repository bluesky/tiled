from __future__ import annotations

import copy
from pathlib import Path
from typing import TYPE_CHECKING, Any
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
        _ = awkward.to_parquet(array._impl, uri)  # noqa: SLF001

    def write_block(self, array: ragged.array, block: NDBlock) -> None:
        """Write a single block of the ragged array to storage."""
        uri = self._block_paths[block[0]]
        _ = awkward.to_parquet(array._impl, uri)  # noqa: SLF001
