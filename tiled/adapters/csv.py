from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import dask.dataframe
import pandas

from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..structures.table import TableStructure
from ..utils import ensure_uri, path_from_uri
from .array import ArrayAdapter
from .dataframe import DataFrameAdapter
from .protocols import AccessPolicy
from .table import TableAdapter
from .type_alliases import JSON


def read_csv(
    data_uri: str,
    structure: Optional[TableStructure] = None,
    metadata: Optional[JSON] = None,
    specs: Optional[List[Spec]] = None,
    access_policy: Optional[AccessPolicy] = None,
    **kwargs: Any,
) -> TableAdapter:
    """
    Read a CSV.

    Internally, this uses dask.dataframe.read_csv.
    It forward all parameters to that function. See
    https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.read_csv

    Examples
    --------

    >>> read_csv("myfiles.*.csv")
    >>> read_csv("s3://bucket/myfiles.*.csv")

    Parameters
    ----------
    data_uri :
    structure :
    metadata :
    specs :
    access_policy :
    kwargs :

    Returns
    -------
    """
    filepath = path_from_uri(data_uri)
    ddf = dask.dataframe.read_csv(filepath, **kwargs)
    # TODO Pass structure through rather than just re-creating it
    # in from_dask_dataframe.
    return DataFrameAdapter.from_dask_dataframe(
        ddf, metadata=metadata, specs=specs, access_policy=access_policy
    )


read_csv.__doc__ = """
This wraps dask.dataframe.read_csv. Original docstring:

""" + (
    dask.dataframe.read_csv.__doc__ or ""
)


class CSVAdapter:
    """ """

    structure_family = StructureFamily.table

    def __init__(
        self,
        data_uris: List[str],
        structure: Optional[TableStructure] = None,
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
        if structure is None:
            table = dask.dataframe.read_csv(self._partition_paths)
            structure = TableStructure.from_dask_dataframe(table)
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
    def dataframe_adapter(self) -> TableAdapter:
        """

        Returns
        -------

        """
        partitions = []
        for path in self._partition_paths:
            if not Path(path).exists():
                partition = None
            else:
                partition = dask.dataframe.read_csv(path)
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
                data_uri=f"{data_uri}/partition-{i}.csv",
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
            for i in range(structure.npartitions)
        ]
        return assets

    def append_partition(
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
        data.to_csv(uri, index=False, mode="a", header=False)

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
        data.to_csv(uri, index=False)

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
        data.to_csv(uri, index=False)

    def read(
        self, *args: Any, **kwargs: Any
    ) -> Union[pandas.DataFrame, dask.dataframe.DataFrame]:
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
        """

        Parameters
        ----------
        key :

        Returns
        -------

        """
        if key not in self.structure().columns:
            return None
        return ArrayAdapter.from_array(self.read([key])[key].values)

    def generate_data_sources(
        self,
        mimetype: str,
        dict_or_none: Callable[[TableStructure], Dict[str, str]],
        item: Union[str, Path],
        is_directory: bool,
    ) -> List[DataSource]:
        """

        Parameters
        ----------
        mimetype :
        dict_or_none :
        item :
        is_directory :

        Returns
        -------

        """
        return [
            DataSource(
                structure_family=self.dataframe_adapter.structure_family,
                mimetype=mimetype,
                structure=dict_or_none(self.dataframe_adapter.structure()),
                parameters={},
                management=Management.external,
                assets=[
                    Asset(
                        data_uri=ensure_uri(item),
                        is_directory=is_directory,
                        parameter="data_uris",  # <-- PLURAL!
                        num=0,  # <-- denoting that the Adapter expects a list, and this is the first element
                    )
                ],
            )
        ]

    @classmethod
    def from_single_file(
        cls,
        data_uri: str,
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "CSVAdapter":
        """

        Parameters
        ----------
        data_uri :
        structure :
        metadata :
        specs :
        access_policy :

        Returns
        -------

        """
        return cls(
            [data_uri],
            structure=structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

    def __getitem__(self, key: str) -> ArrayAdapter:
        """

        Parameters
        ----------
        key :

        Returns
        -------

        """
        # Must compute to determine shape.
        return ArrayAdapter.from_array(self.read([key])[key].values)

    def items(self) -> Iterator[Tuple[str, ArrayAdapter]]:
        """

        Returns
        -------

        """
        yield from (
            (key, ArrayAdapter.from_array(self.read([key])[key].values))
            for key in self._structure.columns
        )
