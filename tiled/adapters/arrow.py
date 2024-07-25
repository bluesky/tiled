from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import dask.dataframe
import pandas
import pyarrow
import pyarrow.feather as feather
import pyarrow.fs

from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..structures.table import TableStructure
from ..utils import ensure_uri, path_from_uri
from .array import ArrayAdapter
from .protocols import AccessPolicy
from .type_alliases import JSON


class ReaderHandle:
    """Class to provide handle to read the data via ArrowAdapter."""

    def __init__(
        self,
        partitions: Union[dask.dataframe.DataFrame, pandas.DataFrame],
    ) -> None:
        """
        Class to create a new instance of read_all function.
        Parameters
        ----------
        partitions : the partitions
        """
        self._partitions = list(partitions)

    def read(self) -> pyarrow.table:
        """
        The concatenated data from given set of partitions as pyarrow table.
        Parameters
        ----------
        Returns
        -------
        Returns the concatenated pyarrow table.
        """
        print("Ever in adapters/table read????", len(self._partitions))
        if any(p is None for p in self._partitions):
            raise ValueError("Not all partitions have been stored.")

        return pyarrow.concat_tables(
            [partition.read_all() for partition in self._partitions]
        )

    def read_partition_with_batch(self, partition: int, batch: int) -> pyarrow.table:
        """
        Function to read a batch of data from a given parititon.
        Parameters
        ----------
        partition : the index of the partition to read.
        batch : the index of the batch to read.

        Returns
        -------
        The pyarrow table corresponding to a given partition and batch.
        """
        df = self._partitions[partition]
        return df.get_batch(batch)


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
        data_uris : list of uris where data sits.
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
        Class to initialize the list of assets for given uri.
        Parameters
        ----------
        data_uri :
        structure :

        Returns
        -------
        The list of assets.
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
                structure_family=self.structure_family,
                mimetype=mimetype,
                structure=dict_or_none(self.structure()),
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

    @property
    def reader_handle(self) -> ReaderHandle:
        """
        Function to initialize and return the reader hanle.
        Returns
        -------
        The reader handle.
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
        return ReaderHandle(partitions)

    def write_partition(
        self,
        data: Union[List[pyarrow.record_batch], pyarrow.record_batch],
        partition: int,
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

        with pyarrow.ipc.new_file(uri, schema) as file_writer:
            for ibatch in data:
                file_writer.write_batch(ibatch)
            file_writer.close()

    def write(
        self, data: Union[List[pyarrow.record_batch], pyarrow.record_batch]
    ) -> None:
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

        with pyarrow.ipc.new_file(uri, schema) as file_writer:
            for ibatch in data:
                file_writer.write_batch(ibatch)
            file_writer.close()

    def read(self, *args: Any, **kwargs: Any) -> pyarrow.table:
        """
        Function to read all the partitions of the data.
        Parameters
        ----------
        args : any extra arguments to be unpacked into the function.
        kwargs : any extra keyword arguments to be unpacked into the function.

        Returns
        -------
        The whole content of the file as pyarrow table.
        """
        return self.reader_handle.read(*args, **kwargs)

    def read_partition(self, *args: Any, **kwargs: Any) -> pyarrow.table:
        """
        Function to read a batch of data from a given parititon.
        Parameters
        ----------
        args : any extra arguments to be unpacked into the function.
        kwargs : any extra keyword arguments to be unpacked into the function.

        Returns
        -------
        The pyarrow table corresponding to given partition and batch.
        """
        return self.reader_handle.read_partition_with_batch(*args, **kwargs)
