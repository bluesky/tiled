from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import dask.dataframe
import pandas
import pyarrow
import pyarrow.feather as feather

from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..structures.table import TableStructure
from ..utils import ensure_uri, path_from_uri
from .array import ArrayAdapter
from .dataframe import DataFrameAdapter
from .protocols import AccessPolicy
from .table import TableAdapter
from .type_alliases import JSON


class ArrowAdapter:
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
            # table = dask.dataframe.read_csv(self._partition_paths)
            table = feather.read_table(self._partition_paths)
            # structure = TableStructure.from_dask_dataframe(table)
            structure = TableStructure.from_arrow_table(table)
        self._structure = structure
        self.specs = list(specs or [])
        self.access_policy = access_policy

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        return self._metadata

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
                data_uri=f"{data_uri}/partition-{i}.arrow",
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
            for i in range(structure.npartitions)
        ]
        return assets

    def structure(self) -> TableStructure:
        """

        Returns
        -------

        """
        return self._structure

    # def get(self, key: str) -> Union[ArrayAdapter, None]:
    #    """
    #
    #    Parameters
    #    ----------
    #    key :
    #
    #    Returns
    #    -------
    #
    #    """
    #    if key not in self.structure().columns:
    #        return None
    #    return ArrayAdapter.from_array(self.read([key])[key].values)

    # def generate_data_sources(
    #     self,
    #     mimetype: str,
    #     dict_or_none: Callable[[TableStructure], Dict[str, str]],
    #     item: Union[str, Path],
    #     is_directory: bool,
    # ) -> List[DataSource]:
    #     """
    #
    #     Parameters
    #     ----------
    #     mimetype :
    #     dict_or_none :
    #     item :
    #     is_directory :
    #
    #     Returns
    #     -------
    #
    #     """
    #     return [
    #         DataSource(
    #             structure_family=self.dataframe_adapter.structure_family,
    #             mimetype=mimetype,
    #             structure=dict_or_none(self.dataframe_adapter.structure()),
    #             parameters={},
    #             management=Management.external,
    #             assets=[
    #                 Asset(
    #                     data_uri=ensure_uri(item),
    #                     is_directory=is_directory,
    #                     parameter="data_uris",  # <-- PLURAL!
    #                     num=0,  # <-- denoting that the Adapter expects a list, and this is the first element
    #                 )
    #             ],
    #         )
    #     ]
    #
    @classmethod
    def from_single_file(
        cls,
        data_uri: str,
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "ArrowAdapter":
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

    # def __getitem__(self, key: str) -> ArrayAdapter:
    #    """
    #
    #    Parameters
    #    ----------
    #    key :
    #
    #    Returns
    #    -------
    #
    #    """
    #    # Must compute to determine shape.
    #    return ArrayAdapter.from_array(self.read([key])[key].values)

    # def items(self) -> Iterator[Tuple[str, ArrayAdapter]]:
    #    """
    #
    #    Returns
    #    -------
    #
    #    """
    #    yield from (
    #        (key, ArrayAdapter.from_array(self.read([key])[key].values))
    #        for key in self._structure.columns
    #    )


class ArrowAdapterStream(ArrowAdapter):
    def __init__(
        self,
        data_uris: List[str],
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        super().__init__(
            data_uris=data_uris,
            structure=structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

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
                # partition = dask.dataframe.read_csv(path)
                # with pyarrow.ipc.open_file(path) as reader:
                with pyarrow.ipc.open_stream(path) as reader:
                    partition = reader
            partitions.append(partition)
        return DataFrameAdapter(partitions, self._structure)

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
        print("HELL0 URI In APPEND", type(uri))
        self.stream_writer.write_batch(data)
        # self.stream_writer.close()

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
        if isinstance(data, list):
            schema = data[0].schema
        else:
            schema = data.schema

        uri = self._partition_paths[partition]
        if not hasattr(self, "stream_writer"):
            self.stream_writer = pyarrow.ipc.new_stream(uri, schema)

        self.stream_writer.write_batch(data)

    def write(self, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]) -> None:
        """

        Parameters
        ----------
        data :

        Returns
        -------

        """
        if isinstance(data, list):
            schema = data[0].schema
        else:
            schema = data.schema

        if self.structure().npartitions != 1:
            raise NotImplementedError
        uri = self._partition_paths[0]
        if not hasattr(self, "stream_writer"):
            self.stream_writer = pyarrow.ipc.new_stream(uri, schema)
        self.stream_writer.write_batch(data)

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


class ArrowAdapterRandomAccess(ArrowAdapter):
    def __init__(
        self,
        data_uris: List[str],
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        super().__init__(
            data_uris=data_uris,
            structure=structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

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
                # partition = dask.dataframe.read_csv(path)
                with pyarrow.ipc.open_file(path) as reader:
                    # with pyarrow.ipc.open_stream(path) as reader:
                    partition = reader
            partitions.append(partition)
        return DataFrameAdapter(partitions, self._structure)

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
        print("HELL0 URI In APPEND", type(uri))
        self.file_writer.write_batch(data)
        # self.file_writer.close()

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
        if isinstance(data, list):
            schema = data[0].schema
        else:
            schema = data.schema

        uri = self._partition_paths[partition]
        if not hasattr(self, "stream_writer"):
            self.file_writer = pyarrow.ipc.new_file(uri, schema)

        self.file_writer.write_batch(data)
        # self.file_writer.close()

    def write(self, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]) -> None:
        """

        Parameters
        ----------
        data :

        Returns
        -------

        """
        if isinstance(data, list):
            schema = data[0].schema
        else:
            schema = data.schema

        if self.structure().npartitions != 1:
            raise NotImplementedError
        uri = self._partition_paths[0]
        if not hasattr(self, "file_writer"):
            self.file_writer = pyarrow.ipc.new_file(uri, schema)
        self.file_writer.write_batch(data)
        # self.file_writer.close()

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
        self.file_writer.close()
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
        self.file_writer.close()
        return self.dataframe_adapter.read_partition(*args, **kwargs)
