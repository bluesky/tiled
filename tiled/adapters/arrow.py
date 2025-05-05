import copy
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import quote_plus

import pandas
import pyarrow
import pyarrow.feather as feather
import pyarrow.fs

from ..catalog.orm import Node
from ..storage import FileStorage, Storage
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..structures.table import TableStructure
from ..type_aliases import JSON
from ..utils import ensure_uri, path_from_uri
from .array import ArrayAdapter
from .utils import init_adapter_from_catalog


class ArrowAdapter:
    """ArrowAdapter Class"""

    structure_family = StructureFamily.table
    supported_storage = {FileStorage}

    def __init__(
        self,
        data_uris: List[str],
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        **kwargs: Optional[Any],
    ) -> None:
        """

        Parameters
        ----------
        data_uris : list of uris where data sits.
        structure :
        metadata :
        specs :
        """
        # TODO Store data_uris instead and generalize to non-file schemes.
        self._partition_paths = [path_from_uri(uri) for uri in data_uris]
        self._metadata = metadata or {}
        if structure is None:
            table = feather.read_table(self._partition_paths)
            structure = TableStructure.from_arrow_table(table)
        self._structure = structure
        self.specs = list(specs or [])

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[TableStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "ArrowAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)  # type: ignore

    def metadata(self) -> JSON:
        return self._metadata

    @classmethod
    def init_storage(
        cls,
        storage: Storage,
        data_source: DataSource[TableStructure],
        path_parts: List[str],
    ) -> DataSource[TableStructure]:
        """
        Class to initialize the list of assets for given uri.
        Parameters
        ----------
        storage : the storage option for .arrow files
        data_source : data source representing the adapter
        path_parts: the list of partitions
        Returns
        -------
        The list of assets.
        """
        data_source = copy.deepcopy(data_source)  # Do not mutate caller input.
        data_uri = storage.uri + "".join(
            f"/{quote_plus(segment)}" for segment in path_parts
        )

        directory = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        assets = [
            Asset(
                data_uri=f"{data_uri}/partition-{i}.arrow",
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
            for i in range(data_source.structure.npartitions)
        ]
        data_source.assets.extend(assets)
        return data_source

    def structure(self) -> TableStructure:
        return self._structure

    def get(self, key: str) -> Union[ArrayAdapter, None]:
        if key not in self.structure().columns:
            return None
        return ArrayAdapter.from_array(self.read([key])[key].values)

    def generate_data_sources(
        self,
        mimetype: str,
        dict_or_none: Callable[[TableStructure], Dict[str, str]],
        item: Union[str, Path],
        is_directory: bool,
    ) -> List[DataSource[TableStructure]]:
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
                structure_family=self.structure_family,
                mimetype=mimetype,
                structure=self.structure(),
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

    #
    @classmethod
    def from_single_file(
        cls,
        data_uri: str,
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> "ArrowAdapter":
        """

        Parameters
        ----------
        data_uri :
        structure :
        metadata :
        specs :

        Returns
        -------

        """
        return cls(
            [data_uri],
            structure=structure,
            metadata=metadata,
            specs=specs,
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
        yield from (
            (key, ArrayAdapter.from_array(self.read([key])[key].values))
            for key in self._structure.columns
        )

    def reader_handle_partiton(self, partition: int) -> pyarrow.RecordBatchFileReader:
        """
        Function to initialize and return the reader handle.
        Parameters
        ----------
        partition : the integer number corresponding to a specific partition.
        Returns
        -------
        The reader handle for specific partition.
        """
        if not Path(self._partition_paths[partition]).exists():
            raise ValueError(f"partition {partition} has not been stored yet")
        else:
            return pyarrow.ipc.open_file(self._partition_paths[partition])

    def reader_handle_all(self) -> Iterator[pyarrow.RecordBatchFileReader]:
        """
        Function to initialize and return the reader handle.
        Returns
        -------
        The reader handle.
        """
        for path in self._partition_paths:
            if not Path(path).exists():
                raise ValueError(f"path {path} has not been stored yet")
            else:
                with pyarrow.ipc.open_file(path) as reader:
                    yield reader

    def write_partition(
        self,
        data: Union[List[pyarrow.record_batch], pyarrow.record_batch, pandas.DataFrame],
        partition: int,
    ) -> None:
        """
        "Function to write the data into specific partition as arrow format."
        Parameters
        ----------
        data : data to write into arrow file. Can be a list of record batch, or pandas dataframe.
        partition: integer index of partition to be read.
        Returns
        -------
        """
        if isinstance(data, pandas.DataFrame):
            table = pyarrow.Table.from_pandas(data)
            batches = table.to_batches()
        else:
            if not isinstance(data, list):
                batches = [data]
            else:
                batches = data

        schema = batches[0].schema

        uri = self._partition_paths[partition]

        with pyarrow.ipc.new_file(uri, schema) as file_writer:
            for batch in batches:
                file_writer.write_batch(batch)

    def write(
        self,
        data: Union[List[pyarrow.record_batch], pyarrow.record_batch, pandas.DataFrame],
    ) -> None:
        """
        "Function to write the data as arrow format."
        Parameters
        ----------
        data : data to write into arrow file. Can be a list of record batch, or pandas dataframe.
        Returns
        -------
        """
        if isinstance(data, pandas.DataFrame):
            table = pyarrow.Table.from_pandas(data)
            batches = table.to_batches()
        else:
            if not isinstance(data, list):
                batches = [data]
            else:
                batches = data

        schema = batches[0].schema

        if self.structure().npartitions != 1:
            raise NotImplementedError
        uri = self._partition_paths[0]

        with pyarrow.ipc.new_file(uri, schema) as file_writer:
            for batch in data:
                file_writer.write_batch(batch)

    def read(self, fields: Optional[Union[str, List[str]]] = None) -> pandas.DataFrame:
        """
        The concatenated data from given set of partitions as pyarrow table.
        Parameters
        ----------
        Returns
        -------
        Returns the concatenated pyarrow table as pandas dataframe.
        """
        data = pyarrow.concat_tables(
            [partition.read_all() for partition in self.reader_handle_all()]
        )
        table = data.to_pandas()
        if fields is not None:
            return table[fields]
        return table

    def read_partition(
        self,
        partition: int,
        fields: Optional[Union[str, List[str]]] = None,
    ) -> pandas.DataFrame:
        """
        Function to read a batch of data from a given partition.
        Parameters
        ----------
        partition : the index of the partition to read.
        fields : optional fields parameter.

        Returns
        -------
        The pyarrow table corresponding to a given partition and batch as pandas dataframe.
        """
        reader = self.reader_handle_partiton(partition)
        table = reader.read_all().to_pandas()
        if fields is not None:
            return table[fields]
        return table
