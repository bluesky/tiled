from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus

import awkward
import dask_awkward
import ragged

from tiled.adapters.core import Adapter
from tiled.adapters.utils import init_adapter_from_catalog
from tiled.ndslice import NDSlice
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

    def read(self, slice: NDSlice | None = None) -> ragged.array:
        """Read ragged array data from storage."""
        if self._structure.npartitions == 1:
            # TODO: I can't get dask_awkward to behave with single-partition data
            data = awkward.from_parquet(str(self._block_paths[0]))
            sliced_data = data[tuple(slice)] if slice else data
            return ragged.array(sliced_data)

        data = dask_awkward.from_parquet([str(path) for path in self._block_paths])
        if isinstance(data, tuple):
            raise RuntimeError(
                "dask_awkward.from_parquet produced unexpected pair of arrays"
            )
        data = data.persist()
        sliced_data = data[tuple(slice)] if slice else data
        sliced_data = sliced_data.persist()
        return ragged.array(dask_awkward.to_packed(sliced_data).compute())

    def read_block(self, block: int, slice: NDSlice | None = None) -> ragged.array:
        """Read a single block of the ragged array from storage."""
        data = dask_awkward.from_parquet([str(path) for path in self._block_paths])
        if isinstance(data, tuple):
            raise RuntimeError(
                "dask_awkward.from_parquet produced unexpected pair of arrays"
            )
        part = data.partitions[block].persist()
        sliced_data = part[tuple(slice)] if slice else part
        sliced_data = sliced_data.persist()
        return ragged.array(dask_awkward.to_packed(sliced_data).compute())

    def write(self, array: ragged.array) -> None:
        """Write ragged array data to storage."""
        if self._structure.npartitions != 1:
            raise NotImplementedError
        uri = self._block_paths[0]
        _ = awkward.to_parquet(array._impl, uri)  # noqa: SLF001

    def write_block(self, array: ragged.array, block: int) -> None:
        """Write a single block of the ragged array to storage."""
        uri = self._block_paths[block]
        _ = awkward.to_parquet(array._impl, uri)  # noqa: SLF001
