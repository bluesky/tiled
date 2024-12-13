import copy
import hashlib
import uuid
from typing import Iterator, List, Optional, Tuple, Union

import adbc_driver_postgresql.dbapi
import adbc_driver_sqlite.dbapi
import pandas
import pyarrow
import pyarrow.fs

from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Storage
from ..structures.table import TableStructure
from ..type_alliases import JSON
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
        # TODO Store data_uris instead and generalize to non-file schemes.
        self.uri = data_uri

        if self.uri.startswith("sqlite"):
            self.conn = adbc_driver_sqlite.dbapi.connect(
                self.uri.removeprefix("sqlite://")
            )
        elif self.uri.startswith("postgresql"):
            self.conn = adbc_driver_postgresql.dbapi.connect(self.uri)
        else:
            raise ValueError(
                "The database uri must start with either `sqlite://` or `postgresql://` "
            )

        self.cur = self.conn.cursor()

        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])
        self.access_policy = access_policy

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
        data_uri : the uri of the data
        structure : the structure of the data

        Returns
        -------
        A modified copy of the data source
        """
        data_source = copy.deepcopy(data_source)  # Do not mutate caller input.
        if data_source.structure.npartitions > 1:
            raise ValueError("The SQL adapter must have only 1 partition")
        default_table_name = ...  # based on hash of Arrow schema
        data_source.parameters.setdefault("table_name", default_table_name)
        data_source.parameters["dataset_id"] = uuid.uuid4().int
        data_uri = storage.get("sql")  # TODO scrub credentials
        data_source.assets.append(
            Asset(
                data_uri=data_uri,
                is_directory=False,
                parameter="data_uris",
                num=0,
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
        table_name: string indicating the name of the table to ingest data in the database.
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

        schema = batches[
            0
        ].schema  # list of column names can be obtained from schema.names
        encoded = schema.serialize()
        table_name = hashlib.md5(encoded, usedforsecurity=True).hexdigest()
        table_name = "table_" + table_name

        reader = pyarrow.ipc.RecordBatchReader.from_batches(schema, batches)

        query = "DROP TABLE IF EXISTS {}".format(table_name)
        self.cur.execute(query)
        self.cur.adbc_ingest(table_name, reader)
        self.conn.commit()

    def append(
        self,
        data: Union[List[pyarrow.record_batch], pyarrow.record_batch, pandas.DataFrame],
    ) -> None:
        """
        "Function to write the data as arrow format."
        Parameters
        ----------
        data : data to append into the database. Can be a list of record batch, or pandas dataframe.
        table_name: string indicating the name of the table to ingest data in the database.
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

        schema = batches[
            0
        ].schema  # list of column names can be obtained from schema.names
        encoded = schema.serialize()
        table_name = hashlib.md5(encoded, usedforsecurity=True).hexdigest()
        table_name = "table_" + table_name

        reader = pyarrow.ipc.RecordBatchReader.from_batches(schema, batches)

        self.cur.adbc_ingest(table_name, reader, mode="append")
        self.conn.commit()

    def read(self, fields: Optional[Union[str, List[str]]] = None) -> pandas.DataFrame:
        """
        The concatenated data from given set of partitions as pyarrow table.
        Parameters
        ----------
        table_schema: hashed string or list of strings as column names to be hashed.
                      for example table_schema = ['f0', 'f1', 'f2'] or '3d51c6b180b64bea848f23e5crd91ea3'
        fields: optional string to return the data in the specified field.
        Returns
        -------
        Returns the concatenated pyarrow table as pandas dataframe.
        """
        encoded = pyarrow.schema(self._structure.arrow_schema_decoded).serialize()
        table_name = hashlib.md5(encoded, usedforsecurity=True).hexdigest()
        table_name = "table_" + table_name

        query = "SELECT * FROM {}".format(table_name)
        self.cur.execute(query)
        data = self.cur.fetch_arrow_table()
        self.conn.commit()

        table = data.to_pandas()
        if fields is not None:
            return table[fields]
        return table
