import copy
import hashlib
import os
import secrets
from pathlib import Path
from typing import Iterator, List, Optional, Tuple, Union

import adbc_driver_postgresql.dbapi
import adbc_driver_sqlite.dbapi
import numpy
import pandas
import pyarrow
import pyarrow.fs

from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Storage
from ..structures.table import TableStructure
from ..type_aliases import JSON
from ..utils import path_from_uri
from .array import ArrayAdapter
from .protocols import AccessPolicy


class SQLAdapter:
    """SQLAdapter Class"""

    structure_family = StructureFamily.table

    def __init__(
        self,
        data_uri: str,
        structure: TableStructure,
        table_name: str,
        dataset_id: int,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """
        Construct the SQLAdapter object.
        Parameters
        ----------
        data_uri : the uri of the database, starting either with "sqlite://" or "postgresql://"
        structure : the structure of the data. structure is not optional for sql database
        metadata : the optional metadata of the data.
        specs : the specs.
        access_policy : the access policy of the data.
        """
        self.uri = data_uri

        if self.uri.startswith("sqlite:"):
            # Ensure this path is writable to avoid a confusing error message
            # from abdc_driver_sqlite.
            filepath = path_from_uri(self.uri)
            directory = Path(filepath).parent
            if directory.exists():
                if not os.access(directory, os.X_OK | os.W_OK):
                    raise ValueError(
                        f"The directory {directory} exists but is not writable and executable."
                    )
                if Path(filepath).is_file() and (not os.access(filepath, os.W_OK)):
                    raise ValueError(f"The path {filepath} exists but is not writable.")
            else:
                raise ValueError(f"The directory {directory} does not exist.")
            self.conn = adbc_driver_sqlite.dbapi.connect(str(filepath))
        elif self.uri.startswith("postgresql:"):
            self.conn = adbc_driver_postgresql.dbapi.connect(self.uri)
        else:
            raise ValueError(
                "The database uri must start with either `sqlite:` or `postgresql:` "
            )

        self.cur = self.conn.cursor()

        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])
        self.access_policy = access_policy
        self.table_name = table_name
        self.dataset_id = dataset_id

    def metadata(self) -> JSON:
        """
        The metadata representing the actual data.
        Returns
        -------
        The metadata representing the actual data.
        """
        return self._metadata

    @classmethod
    def init_storage(
        cls,
        storage: Storage,
        data_source: DataSource[TableStructure],
        path_parts: List[str],
    ) -> DataSource[TableStructure]:
        """
        Class to initialize the list of assets for given uri. In SQL Adapter we hve  single partition.

        Parameters
        ----------
        storage: the storage kind
        data_source : data source describing the data
        path_parts: the list of partitions.
        Returns
        -------
        A modified copy of the data source
        """
        data_source = copy.deepcopy(data_source)  # Do not mutate caller input.
        if data_source.structure.npartitions > 1:
            raise ValueError("The SQL adapter must have only 1 partition")

        schema = (
            data_source.structure.arrow_schema_decoded
        )  # based on hash of Arrow schema
        encoded = schema.serialize()
        default_table_name = "table_" + hashlib.md5(encoded).hexdigest()
        data_source.parameters.setdefault("table_name", default_table_name)
        data_source.parameters["dataset_id"] = secrets.randbits(63)
        data_uri = storage.get("sql")  # TODO scrub credentials
        data_source.assets.append(
            Asset(
                data_uri=data_uri,
                is_directory=False,
                parameter="data_uri",
                num=None,
            )
        )
        return data_source

    def structure(self) -> TableStructure:
        """
        The structure of the actual data.
        Returns
        -------
        The structure of the data.
        """
        return self._structure

    def get(self, key: str) -> Union[ArrayAdapter, None]:
        """
        Get the data for a specific key
        Parameters
        ----------
        key : a string to indicate which column to be retrieved
        Returns
        -------
        The column for the associated key.
        """
        if key not in self.structure().columns:
            return None
        return ArrayAdapter.from_array(self.read([key])[key].values)

    def __getitem__(self, key: str) -> ArrayAdapter:
        """
        Get the data for a specific key.
        Parameters
        ----------
        key : a string to indicate which column to be retrieved
        Returns
        -------
        The column for the associated key.
        """
        # Must compute to determine shape.
        return ArrayAdapter.from_array(self.read([key])[key].values)

    def items(self) -> Iterator[Tuple[str, ArrayAdapter]]:
        """
        The function to iterate over the SQLAdapter data.
        Returns
        -------
        An iterator for the data in the associated database.
        """
        yield from (
            (key, ArrayAdapter.from_array(self.read([key])[key].values))
            for key in self._structure.columns
        )

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
        else:
            if not isinstance(data, list):
                batches = [data]
            else:
                batches = data
            table = pyarrow.Table.from_batches(batches)
        table_with_dataset_id = add_dataset_column(table, self.dataset_id)

        # Delete any existing rows from this table for this dataset_id.
        # query = f"DELETE FROM {self.table_name} WHERE dataset_id={self.dataset_id}"
        # self.cur.execute(query)
        self.cur.adbc_ingest(
            self.table_name, table_with_dataset_id, mode="create_append"
        )
        self.conn.commit()

    def write_partition(
        self,
        partition: int,
        data: Union[List[pyarrow.record_batch], pyarrow.record_batch, pandas.DataFrame],
    ) -> None:
        """
        "Function to write the data as arrow format."
        Parameters
        ----------
        partition : the partition index to write.
        data : data to write into arrow file. Can be a list of record batch, or pandas dataframe.
        Returns
        -------
        """
        if partition != 0:
            raise NotImplementedError
        return self.write(data)

    def append_partition(
        self,
        data: Union[List[pyarrow.record_batch], pyarrow.record_batch, pandas.DataFrame],
        partition: int,
    ) -> None:
        """
        "Function to write the data as arrow format."

        Parameters
        ----------
        data : data to append into the database. Can be a list of record batch, or pandas dataframe.
        partition : the partition index to write.
        Returns
        -------
        """
        if isinstance(data, pandas.DataFrame):
            table = pyarrow.Table.from_pandas(data)
        else:
            if not isinstance(data, list):
                batches = [data]
            else:
                batches = data
            table = pyarrow.Table.from_batches(batches)
        table_with_dataset_id = add_dataset_column(table, self.dataset_id)

        self.cur.adbc_ingest(self.table_name, table_with_dataset_id, mode="append")
        self.conn.commit()

    def read(self, fields: Optional[Union[str, List[str]]] = None) -> pandas.DataFrame:
        """
        The concatenated data from given set of partitions as pyarrow table.
        Parameters
        ----------
        fields: optional string to return the data in the specified field.
        Returns
        -------
        Returns the concatenated pyarrow table as pandas dataframe.
        """

        query = f"SELECT * FROM {self.table_name} WHERE dataset_id={self.dataset_id}"
        self.cur.execute(query)
        data = self.cur.fetch_arrow_table()
        self.conn.commit()

        table = data.to_pandas()
        if fields is not None:
            return table[fields]
        return table.drop("dataset_id", axis=1)

    def read_partition(
        self, partition: int, fields: Optional[Union[str, List[str]]] = None
    ) -> pandas.DataFrame:
        """
        The concatenated data from given set of partitions as pyarrow table.
        Parameters
        ----------
        partition : the partition index to write.
        fields: optional string to return the data in the specified field.
        Returns
        -------
        Returns the concatenated pyarrow table as pandas dataframe.
        """
        if partition != 0:
            raise NotImplementedError
        return self.read(fields)


def add_dataset_column(table: pyarrow.Table, dataset_id: int) -> pyarrow.Table:
    column = dataset_id * numpy.ones(len(table), dtype=numpy.int64)
    return table.add_column(0, pyarrow.field("dataset_id", pyarrow.int64()), [column])
