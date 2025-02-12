import copy
import hashlib
import os
import re
import secrets
from pathlib import Path
from typing import Any, Iterator, List, Optional, Tuple, Union

import adbc_driver_postgresql.dbapi
import adbc_driver_sqlite.dbapi
import numpy
import pandas
import pyarrow
import pyarrow.fs
from sqlalchemy.sql.compiler import RESERVED_WORDS

from ..catalog.orm import Node
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Storage
from ..structures.table import TableStructure
from ..type_aliases import JSON
from ..utils import path_from_uri
from .array import ArrayAdapter
from .protocols import AccessPolicy
from .utils import init_adapter_from_catalog


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

        self.conn = create_connection(self.uri)
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
        table_name = data_source.parameters.setdefault("table_name", default_table_name)
        check_table_name(table_name)

        dataset_id = secrets.randbits(63)
        data_source.parameters["dataset_id"] = dataset_id
        data_uri = storage.get("sql")  # TODO scrub credential

        schema_new = schema.insert(0, pyarrow.field("dataset_id", pyarrow.int64()))
        create_table_statement = schema_to_pg_create_table(schema_new, table_name)

        create_index_statement = (
            "CREATE INDEX IF NOT EXISTS dataset_id_index "
            f"ON {table_name}(dataset_id)"
        )
        conn = create_connection(data_uri)
        conn.cursor().execute(create_table_statement)
        conn.cursor().execute(create_index_statement)
        conn.commit()

        data_source.assets.append(
            Asset(
                data_uri=data_uri,
                is_directory=False,
                parameter="data_uri",
                num=None,
            )
        )
        return data_source

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[TableStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "SQLAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)  # type: ignore

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


def create_connection(uri: str) -> adbc_driver_sqlite.dbapi.AdbcSqliteConnection:
    if uri.startswith("sqlite:"):
        # Ensure this path is writable to avoid a confusing error message
        # from abdc_driver_sqlite.
        filepath = path_from_uri(uri)
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
        conn = adbc_driver_sqlite.dbapi.connect(str(filepath))
    elif uri.startswith("postgresql:"):
        conn = adbc_driver_postgresql.dbapi.connect(uri)
    else:
        raise ValueError(
            "The database uri must start with either `sqlite:` or `postgresql:` "
        )
    return conn


def add_dataset_column(table: pyarrow.Table, dataset_id: int) -> pyarrow.Table:
    column = dataset_id * numpy.ones(len(table), dtype=numpy.int64)
    return table.add_column(0, pyarrow.field("dataset_id", pyarrow.int64()), [column])


def schema_to_pg_create_table(schema: pyarrow.Schema, table_name: str) -> str:
    # Comprehensive mapping of PyArrow types to PostgreSQL types
    type_mapping = {
        # Numeric Types
        "int8": "SMALLINT",  # Could also use "TINYINT" but not native to PG
        "int16": "SMALLINT",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "uint8": "SMALLINT",
        "uint16": "INTEGER",
        "uint32": "BIGINT",
        "uint64": "NUMERIC",  # No unsigned in PG, so use NUMERIC
        "float16": "REAL",
        "float32": "REAL",
        "float64": "DOUBLE PRECISION",
        "decimal128": "DECIMAL",
        "decimal256": "DECIMAL",
        # String Types
        "string": "TEXT",
        "large_string": "TEXT",
        # Binary Types
        "binary": "BYTEA",
        "large_binary": "BYTEA",
        # Boolean Type
        "bool": "BOOLEAN",
        # Temporal Types
        "date32": "DATE",
        "date64": "DATE",
        "timestamp[s]": "TIMESTAMP",
        "timestamp[ms]": "TIMESTAMP",
        "timestamp[us]": "TIMESTAMP",
        "timestamp[ns]": "TIMESTAMP",
        "time32[s]": "TIME",
        "time32[ms]": "TIME",
        "time64[us]": "TIME",
        "time64[ns]": "TIME",
        # Interval Types
        "interval[s]": "INTERVAL",
        "interval[ms]": "INTERVAL",
        "interval[us]": "INTERVAL",
        "interval[ns]": "INTERVAL",
        "interval_month_day_nano": "INTERVAL",
        # List Types - mapped to ARRAY
        "list": "ARRAY",
        "large_list": "ARRAY",
        "fixed_size_list": "ARRAY",
        # Struct Type
        "struct": "JSONB",  # Best approximate in PG
        # Dictionary Type (usually used for categorical data)
        "dictionary": "TEXT",  # Stored as its target type
        # Fixed Size Types
        "fixed_size_binary": "BYTEA",
        # Map Type
        "map": "JSONB",  # Best approximate in PG
        # Duration Types
        "duration[s]": "INTERVAL",
        "duration[ms]": "INTERVAL",
        "duration[us]": "INTERVAL",
        "duration[ns]": "INTERVAL",
    }

    def get_sql_type(field: pyarrow.Field) -> str:
        base_type = str(field.type).lower()

        # Handle list types (arrays)
        if base_type.startswith(("list", "large_list", "fixed_size_list")):
            value_type = field.type.value_type
            sql_value_type = type_mapping.get(str(value_type).lower(), "TEXT")
            return f"{sql_value_type}[]"

        # Handle dictionary types
        if base_type.startswith("dictionary"):
            # Use the dictionary value type
            value_type = field.type.value_type
            return type_mapping.get(str(value_type).lower(), "TEXT")

        # Handle decimal types with precision and scale
        if base_type.startswith("decimal"):
            precision = field.type.precision
            scale = field.type.scale
            return f"DECIMAL({precision}, {scale})"

        # Default handling
        return type_mapping.get(base_type, "TEXT")

    # Build column definitions
    columns = []

    for field in schema:
        sql_type = get_sql_type(field)
        nullable = "NULL" if field.nullable else "NOT NULL"
        columns.append(f"{field.name} {sql_type} {nullable}")

    # Construct the CREATE TABLE statement
    create_statement = (
        f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        """
        + ",\n        ".join(columns)
        + """)
    """
    )

    return create_statement


TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")


def check_table_name(table_name: str) -> None:
    if len(table_name) > 63:
        raise ValueError("Table name is too long, max character number is 63!")

    if TABLE_NAME_PATTERN.match(table_name) is None:
        raise ValueError("Illegal table name!")

    if table_name.lower() in RESERVED_WORDS:
        raise ValueError("Reserved SQL keywords are not allowed in the table name!")
