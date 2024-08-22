import hashlib
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Tuple, Union

import adbc_driver_postgresql.dbapi
import adbc_driver_sqlite.dbapi
import pandas
import pyarrow
import pyarrow.fs

from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..structures.table import TableStructure
from ..utils import ensure_uri, path_from_uri
from .array import ArrayAdapter
from .protocols import AccessPolicy
from .type_alliases import JSON


class SQLAdapter:
    """ArrowAdapter Class"""

    structure_family = StructureFamily.table

    def __init__(
        self,
        data_uri: str,
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
        self.uri = data_uri

        # if self.uri.startswith('sqlite'):
        #    self.conn = adbc_driver_sqlite.dbapi.connect(self.uri)
        # else:
        #    self.conn = adbc_driver_postgresql.dbapi.connect(self.uri)

        self.conn = adbc_driver_sqlite.dbapi.connect(self.uri)
        self.cur = self.conn.cursor()

        self._metadata = metadata or {}
        # if structure is None:
        #   table = feather.read_table(self.uri)
        #   structure = TableStructure.from_arrow_table(table)
        self._structure = structure
        # self.specs = list(specs or [])
        # self.access_policy = access_policy

    def metadata(self) -> JSON:
        """
        The metadata representing the actual data.
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

    def get(self, vars: tuple[str, str]) -> Union[ArrayAdapter, None]:
        """

        Parameters
        ----------
        key :

        Returns
        -------

        """
        table_name, key = vars
        if key not in self.structure().columns:
            return None
        return ArrayAdapter.from_array(self.read(table_name, [key])[key].values)

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
    ) -> "SQLAdapter":
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
            data_uri,
            structure=structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

    def __getitem__(self, vars: tuple[str, str]) -> ArrayAdapter:
        """

        Parameters
        ----------
        key :

        Returns
        -------

        """
        table_name, key = vars
        # Must compute to determine shape.
        return ArrayAdapter.from_array(self.read(table_name, [key])[key].values)

    def items(self, table_name: str) -> Iterator[Tuple[str, ArrayAdapter]]:
        """

        Returns
        -------

        """
        yield from (
            (key, ArrayAdapter.from_array(self.read(table_name, [key])[key].values))
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

        schema = batches[0].schema  # list of column names can be obtained from schema.names
        encoded = ("".join(schema.names)).encode(encoding="UTF-8", errors="strict")
        table_name = hashlib.md5(encoded, usedforsecurity=True).hexdigest()
        reader = pyarrow.ipc.RecordBatchReader.from_batches(schema, batches)

        query = "DROP TABLE IF EXISTS [" + table_name + "]"
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

        schema = batches[0].schema  # list of column names can be obtained from schema.names
        encoded = ("".join(schema.names)).encode(encoding="UTF-8", errors="strict")
        table_name = hashlib.md5(encoded, usedforsecurity=True).hexdigest()

        reader = pyarrow.ipc.RecordBatchReader.from_batches(schema, batches)

        self.cur.adbc_ingest(table_name, reader, mode="append")
        self.conn.commit()

    def read(
        self, table_schema: Union[str, list[str]], fields: Optional[Union[str, List[str]]] = None
    ) -> pandas.DataFrame:
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
        if isinstance(table_schema, List):
            encoded = ("".join(table_schema)).encode(encoding="UTF-8", errors="strict")
            table_name = hashlib.md5(encoded, usedforsecurity=True).hexdigest()
        else:
            table_name = table_schema # we assume it is already hashed in this case

        query = "SELECT * FROM [" + table_name + "]"

        self.cur.execute(query)
        data = self.cur.fetch_arrow_table()
        self.conn.commit()

        table = data.to_pandas()
        if fields is not None:
            return table[fields]
        return table
