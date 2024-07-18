from pathlib import Path
from typing import Any, List, Optional, Union

import dask.dataframe
import pandas

from ..server.schemas import Asset
from ..structures.core import Spec, StructureFamily
from ..structures.table import TableStructure
from ..utils import path_from_uri
from .dataframe import DataFrameAdapter
from .protocols import AccessPolicy
from .type_alliases import JSON


class ParquetDatasetAdapter:
    """ """

    structure_family = StructureFamily.table

    def __init__(
        self,
        data_uris: List[str],
        structure: TableStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """

        Parameters
        ----------
        data_uris :
        structure :
        metadata :
        specs :
        access_policy :
        """
        # TODO Store data_uris instead and generalize to non-file schemes.
        self._partition_paths = [path_from_uri(uri) for uri in data_uris]
        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])
        self.access_policy = access_policy

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        return self._metadata

    @property
    def dataframe_adapter(self) -> DataFrameAdapter:
        """

        Returns
        -------

        """
        partitions = []
        for path in self._partition_paths:
            if not Path(path).exists():
                partition = None
            else:
                partition = dask.dataframe.read_parquet(path)
            partitions.append(partition)
        return DataFrameAdapter(partitions, self._structure)

    @classmethod
    def init_storage(cls, data_uri: str, structure: TableStructure) -> List[Asset]:
        """

        Parameters
        ----------
        data_uri :
        structure :

        Returns
        -------

        """
        directory = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        assets = [
            Asset(
                data_uri=f"{data_uri}/partition-{i}.parquet",
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
            for i in range(structure.npartitions)
        ]
        return assets

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
