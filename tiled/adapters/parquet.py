import copy
from pathlib import Path
from typing import Any, List, Optional, Union
from urllib.parse import quote_plus

import dask.dataframe
import pandas

from ..catalog.orm import Node
from ..storage import FileStorage, Storage
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource
from ..structures.table import TableStructure
from ..type_aliases import JSON
from ..utils import path_from_uri
from .array import ArrayAdapter
from .dataframe import DataFrameAdapter
from .utils import init_adapter_from_catalog


class ParquetDatasetAdapter:
    """ """

    structure_family = StructureFamily.table
    supported_storage = {FileStorage}

    def __init__(
        self,
        data_uris: List[str],
        structure: TableStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        """

        Parameters
        ----------
        data_uris :
        structure :
        metadata :
        specs :
        """
        # TODO Store data_uris instead and generalize to non-file schemes.
        if isinstance(data_uris, str):
            data_uris = [data_uris]
        self._partition_paths = [path_from_uri(uri) for uri in data_uris]
        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[TableStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "ParquetDatasetAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)  # type: ignore

    def metadata(self) -> JSON:
        return self._metadata

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
        """

        Parameters
        ----------
        data_uri :
        structure :

        Returns
        -------

        """
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
        self, data: Union[dask.dataframe.DataFrame, pandas.DataFrame], partition: int
    ) -> None:
        """

        Parameters
        ----------
        data :
        partition :

        Returns
        -------

        """
        uri = self._partition_paths[partition]
        data.to_parquet(uri)

    def write(self, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]) -> None:
        """

        Parameters
        ----------
        data :

        Returns
        -------

        """
        if self.structure().npartitions != 1:
            raise NotImplementedError
        uri = self._partition_paths[0]
        data.to_parquet(uri)

    def read(self, *args: Any, **kwargs: Any) -> pandas.DataFrame:
        """

        Parameters
        ----------
        args :
        kwargs :

        Returns
        -------

        """
        return self.dataframe_adapter.read(*args, **kwargs)

    def read_partition(self, *args: Any, **kwargs: Any) -> pandas.DataFrame:
        """

        Parameters
        ----------
        args :
        kwargs :

        Returns
        -------

        """
        return self.dataframe_adapter.read_partition(*args, **kwargs)

    def structure(self) -> TableStructure:
        """

        Returns
        -------

        """
        return self._structure

    def get(self, key: str) -> Union[ArrayAdapter, None]:
        return self.dataframe_adapter.get(key)
