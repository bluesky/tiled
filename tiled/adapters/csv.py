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
    """Adapter for tabular data stored as partitioned text (csv) files"""

    structure_family = StructureFamily.table

    def __init__(
        self,
        data_uris: Union[str, List[str]],
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
        **kwargs: Optional[Union[str, List[str], Dict[str, str]]],
    ) -> None:
        """Adapter for partitioned tabular data stored as a sequence of text (csv) files

        Parameters
        ----------
        data_uris : list of uris to csv files
        structure :
        metadata :
        specs :
        access_policy :
        kwargs : dict
            any keyword arguments that can be passed to the pandas.read_csv function, e.g. names, sep, dtype, etc.
        """
        # TODO Store data_uris instead and generalize to non-file schemes.
        if isinstance(data_uris, str):
            data_uris = [data_uris]
        self._partition_paths = [path_from_uri(uri) for uri in data_uris]
        self._metadata = metadata or {}
        self._read_csv_kwargs = kwargs
        if structure is None:
            table = dask.dataframe.read_csv(
                self._partition_paths[0], **self._read_csv_kwargs
            )
            structure = TableStructure.from_dask_dataframe(table)
            structure.npartitions = len(self._partition_paths)
        self._structure = structure
        self.specs = list(specs or [])
        self.access_policy = access_policy

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure.columns!r})"

    def metadata(self) -> JSON:
        return self._metadata

    @property
    def dataframe_adapter(self) -> TableAdapter:
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
        """Initialize partitioned csv storage

        Parameters
        ----------
        data_uri : str
            location of the dataset, should point to a folder in which partitioned csv files will be created
        structure : TableStructure
            description of the data structure

        Returns
        -------
            list of assets with each element corresponding to individual partition files
        """
        path_from_uri(data_uri).mkdir(parents=True, exist_ok=True)
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
        """Append data to an existing partition

        Parameters
        ----------
        data : dask.dataframe.DataFrame or pandas.DataFrame
            data to be appended
        partition : int
            index of the partition to be appended to

        """
        uri = self._partition_paths[partition]
        data.to_csv(uri, index=False, mode="a", header=False)

    def write_partition(
        self, data: Union[dask.dataframe.DataFrame, pandas.DataFrame], partition: int
    ) -> None:
        """Write data to a new partition or overwrite an existing one

        Parameters
        ----------
        data : dask.dataframe.DataFrame or pandas.DataFrame
            data to be appended
        partition : int
            index of the partition to be appended to

        """
        uri = self._partition_paths[partition]
        data.to_csv(uri, index=False)

    def write(self, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]) -> None:
        """Default writing function to a dataset with a single partition

        Parameters
        ----------
        data : dask.dataframe.DataFrame or pandas.DataFrame
            data to be written

        """
        if self.structure().npartitions != 1:
            raise NotImplementedError
        uri = self._partition_paths[0]
        data.to_csv(uri, index=False)

    def read(self, fields: Optional[List[str]] = None) -> dask.dataframe.DataFrame:
        """

        Parameters
        ----------
        fields :

        Returns
        -------

        """
        dfs = [
            self.read_partition(i, fields=fields)
            for i in range(len(self._partition_paths))
        ]

        return dask.dataframe.concat(dfs, axis=0)

    def read_partition(
        self,
        indx: int,
        fields: Optional[List[str]] = None,
    ) -> dask.dataframe.DataFrame:
        """Read a single partition

        Parameters
        ----------
        indx : int
            index of the partition to read
        fields :

        Returns
        -------

        """

        df = dask.dataframe.read_csv(
            self._partition_paths[indx], **self._read_csv_kwargs
        )

        if fields is not None:
            df = df[fields]

        return df.compute()

    def structure(self) -> TableStructure:
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

    def __getitem__(self, key: str) -> ArrayAdapter:
        """Get an ArrayAdapter for a single column

        Parameters
        ----------
        key : str
            column name to get

        Returns
        -------
        An array adapter corresponding to a single column in the table.
        """
        # Must compute to determine shape.
        return ArrayAdapter.from_array(self.read([key])[key].values)

    def items(self) -> Iterator[Tuple[str, ArrayAdapter]]:
        """Iterator over table columns

        Returns
        -------
        Tuples of column names and corresponding ArrayAdapters
        """
        yield from (
            (key, ArrayAdapter.from_array(self.read([key])[key].values))
            for key in self._structure.columns
        )
