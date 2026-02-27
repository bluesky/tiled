from __future__ import annotations

import copy
from urllib.parse import quote_plus

import numpy
import ragged

from tiled.adapters.ragged import RaggedAdapter
from tiled.adapters.resource_cache import with_resource_cache
from tiled.ndslice import NDSlice
from tiled.serialization.ragged import from_numpy_array, to_numpy_array
from tiled.storage import FileStorage, Storage
from tiled.structures.core import Spec
from tiled.structures.data_source import Asset, DataSource
from tiled.structures.ragged import RaggedStructure
from tiled.type_aliases import JSON
from tiled.utils import path_from_uri


class RaggedNPYAdapter(RaggedAdapter):
    def __init__(
        self,
        data_uri: str,
        structure: RaggedStructure,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> None:
        """Create an adapter for a ragged array stored as a numpy byte-stream."""
        super().__init__(None, structure, metadata, specs)
        self._filepath = path_from_uri(data_uri)

    @classmethod
    def supported_storage(cls) -> set[type[Storage]]:
        return {FileStorage}

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
        data_source.assets.append(
            Asset(
                data_uri=f"{data_uri}/ragged-data.npy",
                is_directory=False,
                parameter="data_uri",
            ),
        )
        return data_source

    def read(self, slice: NDSlice = NDSlice(...)) -> ragged.array:
        """Read ragged array data from storage."""
        cache_key = (numpy.load, self._filepath)
        data = with_resource_cache(cache_key, numpy.load, self._filepath)
        array = from_numpy_array(
            data,
            self._structure.data_type.to_numpy_dtype(),
            self._structure.offsets,
            self._structure.shape,
        )
        return array[slice] if slice else array

    def write(self, array: ragged.array) -> None:
        """Write ragged array data to storage."""
        data = to_numpy_array(array)
        numpy.save(self._filepath, data)
