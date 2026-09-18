import copy
from collections.abc import Set
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote_plus

import dask.dataframe
import pandas

from tiled.adapters.core import Adapter

from ..catalog.orm import Node
from ..storage import FileStorage, Storage
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..structures.table import TableStructure
from ..type_aliases import JSON
from ..utils import ensure_uri, path_from_uri
from .array import ArrayAdapter
from .dataframe import DataFrameAdapter
from .utils import init_adapter_from_catalog


class ParquetDatasetAdapter(Adapter[TableStructure]):
    structure_family = StructureFamily.table

    def __init__(
        self,
        data_uris: List[str],
        structure: TableStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        # TODO Store data_uris instead and generalize to non-file schemes.
        if isinstance(data_uris, str):
            data_uris = [data_uris]
        self._partition_paths = [path_from_uri(uri) for uri in data_uris]
        super().__init__(structure, metadata=metadata, specs=specs)

    @classmethod
    def supported_storage(cls) -> Set[type[Storage]]:
        return {FileStorage}

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[TableStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "ParquetDatasetAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        **kwargs: Optional[Any],
    ) -> "ParquetDatasetAdapter":
        # Each URI is one partition of a single table; infer the structure from
        # all of them together.
        paths = [path_from_uri(uri) for uri in data_uris]
        ddf = dask.dataframe.read_parquet(paths, **kwargs)
        structure = TableStructure.from_dask_dataframe(ddf)
        return cls(list(data_uris), structure)

    def generate_data_sources(
        self,
        mimetype: str,
        item: Union[str, Path],
        is_directory: bool,
        size: Optional[int] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[DataSource[TableStructure]]:
        return [
            DataSource(
                structure_family=self.structure_family,
                mimetype=mimetype,
                structure=self.structure(),
                parameters=parameters or {},
                management=Management.external,
                assets=[
                    Asset(
                        data_uri=ensure_uri(item),
                        is_directory=is_directory,
                        parameter="data_uris",  # PLURAL: adapter expects a list
                        size=size,
                        num=0,  # this is the first (and here only) partition
                    )
                ],
            )
        ]

    @property
    def dataframe_adapter(self) -> DataFrameAdapter:
        partitions = []
        for path in self._partition_paths:
            if not Path(path).exists():
                partition = None
            else:
                partition = dask.dataframe.read_parquet(path)
            partitions.append(partition)
        return DataFrameAdapter(
            partitions, self._structure, specs=self.specs, metadata=self.metadata()
        )

    @classmethod
    def init_storage(
        cls,
        storage: Storage,
        data_source: DataSource[TableStructure],
        path_parts: List[str],
    ) -> DataSource[TableStructure]:
        data_source = copy.deepcopy(data_source)  # Do not mutate caller input.
        data_uri = storage.uri + "".join(
            f"/{quote_plus(segment)}" for segment in path_parts
        )
        directory = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        assets = [
            Asset(
                data_uri=f"{data_uri}/partition-{i}.parquet",
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
            for i in range(data_source.structure.npartitions)
        ]
        data_source.assets.extend(assets)
        return data_source

    def write_partition(
        self, partition: int, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]
    ) -> None:
        """Write data to a specific partition

        Parameters
        ----------
        partition : int
            index of the partition to be written to
        data : dask.dataframe.DataFrame or pandas.DataFrame
            data to be written

        """
        uri = self._partition_paths[partition]
        data.to_parquet(uri)

    def write(self, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]) -> None:
        if self.structure().npartitions != 1:
            raise NotImplementedError
        uri = self._partition_paths[0]
        data.to_parquet(uri)

    def read(self, *args: Any, **kwargs: Any) -> pandas.DataFrame:
        return self.dataframe_adapter.read(*args, **kwargs)

    def read_partition(self, *args: Any, **kwargs: Any) -> pandas.DataFrame:
        return self.dataframe_adapter.read_partition(*args, **kwargs)

    def get(self, key: str) -> Union[ArrayAdapter, None]:
        return self.dataframe_adapter.get(key)
