import copy
import hashlib
import re
from contextlib import closing
from typing import Any, Callable, Iterator, List, Literal, Optional, Tuple, Union, cast

import numpy
import pandas
import pyarrow
from sqlalchemy.sql.compiler import RESERVED_WORDS

from ..catalog.orm import Node
from ..storage import EmbeddedSQLStorage, RemoteSQLStorage, SQLStorage, get_storage
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource
from ..structures.table import TableStructure
from ..type_aliases import JSON
from .array import ArrayAdapter
from .utils import init_adapter_from_catalog

DIALECTS = Literal["postgresql", "sqlite", "duckdb"]
TABLE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
COLUMN_NAME_PATTERN = re.compile(r"^[a-zA-Z_].*$")
FORBIDDEN_CHARACTERS = re.compile(
    r"""
    (
      '           # single quote -- ends string literals
    | "           # double quote -- ends quoted identifiers (PostgreSQL)
    | `           # backtick -- ends quoted identifiers (MySQL)
    | ;           # semicolon -- terminates statements (SQL injection risk)
    | --          # double dash -- starts comments (SQL injection risk)
    | /\*         # \* -- starts block comment
    | \*/         # */ -- ends block comment
    | \\          # backslash -- escape character (esp. in MySQL)
    | %           # percent -- wildcards (in LIKE patterns, can be tricky if misused)
    | \(          # parenthesis -- alters grouping, can break expressions
    | \)          #
    | =           # equals -- can change logic (injections in expressions)
    | \+          # plus -- can change logic (injections in expressions)
    )
    """,
    re.VERBOSE,
)
# NOTE: While capital letters are allowed in SQL identifiers, we convert column names
# to lower case to avoid potential collisions (in SQLite). The original name with
# upper case letters is retained in the structure, but attempts to create columns
# e.g. "A" and "a" will raise an error.
# Furthermore, user-specified table names can only be in lower case.


class SQLAdapter:
    """SQLAdapter Class

    This class provides an interface for interacting with SQL databases.

    Parameters
    ----------
    data_uri : the uri of the database, starting either with "duckdb://" or "postgresql://"
    structure : the structure of the data; structure is not optional for sql database
    table_name : the name of the table in the database. Will be converted to lower case in
        all SQL queries.
    dataset_id : the dataset id of the data in the storage database.
    metadata : the optional metadata of the data.
    specs : the specs.
    """

    structure_family = StructureFamily.table
    supported_storage = {SQLStorage, EmbeddedSQLStorage, RemoteSQLStorage}

    def __init__(
        self,
        data_uri: str,
        structure: TableStructure,
        table_name: str,
        dataset_id: int,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        self.storage: SQLStorage = cast(SQLStorage, get_storage(data_uri))
        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])
        self.table_name = table_name
        self.dataset_id = dataset_id

    def metadata(self) -> JSON:
        """The metadata representing the actual data.

        Returns
        -------
        The metadata representing the actual data.
        """
        return self._metadata

    @classmethod
    def init_storage(
        cls,
        storage: SQLStorage,
        data_source: DataSource[TableStructure],
        path_parts: Optional[List[str]] = None,
    ) -> DataSource[TableStructure]:
        """
        Class to initialize the list of assets for given uri.

        Parameters
        ----------
        storage: SQLStorage
        data_source : DataSource
        path_parts : List[str]
            Not used by this adapter

        Returns
        -------
        A modified copy of the data source
        """
        data_source = copy.deepcopy(data_source)  # Do not mutate caller input.

        # Create a table_name based on the hash of Arrow schema
        schema = data_source.structure.arrow_schema_decoded
        encoded = schema.serialize()
        default_table_name = "table_" + hashlib.md5(encoded).hexdigest().lower()
        table_name = data_source.parameters.setdefault("table_name", default_table_name)
        is_safe_identifier(table_name, TABLE_NAME_PATTERN, allow_reserved_words=False)

        # Prefix columns with internal _dataset_id, _partition_id, ...
        schema = schema.insert(0, pyarrow.field("_partition_id", pyarrow.int16()))
        schema = schema.insert(0, pyarrow.field("_dataset_id", pyarrow.int32()))
        create_table_statement = arrow_schema_to_create_table(
            schema, table_name, cast(DIALECTS, storage.dialect)
        )

        create_index_statement = (
            "CREATE INDEX IF NOT EXISTS dataset_and_partition_index "
            f'ON "{table_name}"(_dataset_id, _partition_id)'
        )

        with closing(storage.connect()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_statement)
            with conn.cursor() as cursor:
                cursor.execute(create_index_statement)
            # Just once, create a SEQUENCE (or the closest analogue in SQLite) to
            # provide unique dataset_id for each dataset in this database.
            # (If it exists, do nothing.)
            # Then obtain the next value in the SEQUENCE for the dataset we are
            # initializing here.
            if storage.dialect == "sqlite":
                # Create single-row table with a counter, if it does not exist.
                with conn.cursor() as cursor:
                    cursor.execute(
                        "CREATE TABLE IF NOT EXISTS _dataset_id_counter (value INTEGER NOT NULL)"
                    )
                with conn.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO _dataset_id_counter (value) "
                        "SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM _dataset_id_counter)"
                    )
                with conn.cursor() as cursor:
                    # Increment the counter.
                    cursor.execute(
                        "UPDATE _dataset_id_counter SET value = value + 1 "
                        "RETURNING value"
                    )
                    (dataset_id,) = cursor.fetchone()
            else:
                with conn.cursor() as cursor:
                    cursor.execute("CREATE SEQUENCE IF NOT EXISTS _dataset_id_counter")
                with conn.cursor() as cursor:
                    cursor.execute("SELECT nextval('_dataset_id_counter')")
                    (dataset_id,) = cursor.fetchone()
            data_source.parameters["dataset_id"] = dataset_id
            conn.commit()

        data_source.assets.append(
            Asset(
                # Store URI *without* credentials.
                data_uri=storage.uri,
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
        """Get the data for a specific key

        Parameters
        ----------
        key : a string to indicate which column to be retrieved

        Returns
        -------
        The column for the associated key.
        """
        if key not in self.structure().columns:
            return None
        return self[key]

    def __getitem__(self, key: str) -> ArrayAdapter:
        """Get the data for a specific key.

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
        """Iterate over the SQLAdapter data.

        Returns
        -------
        An iterator for the data in the associated database.
        """
        yield from ((key, self[key]) for key in self._structure.columns)

    def append_partition(
        self,
        data: Union[
            List[pyarrow.record_batch],
            pyarrow.record_batch,
            pandas.DataFrame,
            pyarrow.Table,
        ],
        partition: int,
    ) -> None:
        """Write the data as arrow format

        Parameters
        ----------
        data : data to append to the database. Can be a record_batch, a list of record batches, pyarrow table, or a
            pandas dataframe.
        partition : the partition index to write.
        """
        # Convert the data to pyarrow table
        if isinstance(data, pyarrow.Table):
            table = data
        elif isinstance(data, pandas.DataFrame):
            table = pyarrow.Table.from_pandas(data)
        else:
            if not isinstance(data, list):
                batches = [data]
            else:
                batches = data
            table = pyarrow.Table.from_batches(batches)
        # Explicitly cast the table to the structure's schema
        table = table.cast(self.structure().arrow_schema_decoded)
        # Prepend columns for internal dataset_id and partition number.
        dataset_id_column = self.dataset_id * numpy.ones(len(table), dtype=numpy.int32)
        partition_id_column = partition * numpy.ones(len(table), dtype=numpy.int16)
        table = table.add_column(
            0, pyarrow.field("_partition_id", pyarrow.int16()), [partition_id_column]
        )
        table = table.add_column(
            0, pyarrow.field("_dataset_id", pyarrow.int32()), [dataset_id_column]
        )
        # Convert column names to lower case
        if upr_lwr_case_mapping := {
            c: c.lower() for c in table.column_names if c != c.lower()
        }:
            table = table.rename_columns(upr_lwr_case_mapping)

        with closing(self.storage.connect()) as conn:
            with conn.cursor() as cursor:
                cursor.adbc_ingest(self.table_name, table, mode="append")
            conn.commit()

    def _read_full_table_or_partition(
        self, fields: Optional[List[str]] = None, partition: Optional[int] = None
    ) -> pyarrow.Table:
        """Read the data from the database

        This is a helper function to read the data from the database. The result
        is a pyarrow table containing rows either from the entire table or from a
        specific partition. The retained columns are cast to the original type.

        Parameters
        ----------
        fields : optional string to return the data in the specified field.
        partition : optional int to return the data in the specified partition.

        Returns
        -------
        The concatenated table as pyarrow table.
        """

        # Make sure that requested columns exist and safe to put in SQL query.
        schema = self.structure().arrow_schema_decoded
        req_cols = set(schema.names).intersection(fields) if fields else schema.names

        query = (
            "SELECT " + ", ".join([f'"{c.lower()}"' for c in req_cols]) + " "
            f'FROM "{self.table_name}" '
            f"WHERE _dataset_id={self.dataset_id} "
        )
        query += (
            f"AND _partition_id={int(partition)}"
            if partition is not None
            else "ORDER BY _partition_id"
        )

        with closing(self.storage.connect()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetch_arrow_table()
            conn.commit()

        # The database may have stored this in a coarser type, such as
        # storing uint8 data as int16. Cast it to the original type.
        # Additionally, back-convert lower case column names to their original names.
        if lwr_upr_case_mapping := {c.lower(): c for c in req_cols if c != c.lower()}:
            data = data.rename_columns(lwr_upr_case_mapping)
        target_schema = pyarrow.schema(
            [schema.field(schema.get_field_index(name)) for name in req_cols]
        )

        return data.cast(target_schema)

    def read(self, fields: Optional[List[str]] = None) -> pandas.DataFrame:
        """Read the concatenated data from the entire table.

        Parameters
        ----------
        fields: optional string to return the data in the specified field.

        Returns
        -------
        The concatenated table as pandas dataframe.
        """

        return self._read_full_table_or_partition(fields=fields).to_pandas()

    def read_partition(
        self, partition: int, fields: Optional[List[str]] = None
    ) -> pandas.DataFrame:
        """Read a batch of data from a given partition.

        Parameters
        ----------
        partition : int
        fields : Optional[List[str]]
            Optional list of field names to select. By default return all.

        Returns
        -------
        The concatenated table as pandas dataframe.
        """

        return self._read_full_table_or_partition(
            fields=fields, partition=partition
        ).to_pandas()


# Mapping between Arrow types and PostgreSQL column type name.
ARROW_TO_PG_TYPES: dict[pyarrow.Field, str] = {
    # Boolean
    pyarrow.bool_(): "BOOLEAN",
    # Integers
    pyarrow.int8(): "SMALLINT",  # no native int8, use int16
    pyarrow.int16(): "SMALLINT",
    pyarrow.int32(): "INTEGER",
    pyarrow.int64(): "BIGINT",
    # Since PG has not native unsigned int,
    # use signed int one size up.
    pyarrow.uint8(): "SMALLINT",
    pyarrow.uint16(): "INTEGER",
    pyarrow.uint32(): "BIGINT",
    # pyarrow.uint64() not supported
    # Floating Point
    # pyarrow.float16(): "REAL",
    pyarrow.float32(): "REAL",
    pyarrow.float64(): "DOUBLE PRECISION",
    # String Types
    pyarrow.string(): "TEXT",
    pyarrow.large_string(): "TEXT",
    # TODO Consider adding support for these types, with testing.
    # # Binary Types
    # pyarrow.binary(): "bytea",
    # pyarrow.large_binary(): "bytea",
    # # Date/Time Types
    # pyarrow.date32(): "date",
    # pyarrow.date64(): "date",
    # pyarrow.time32("s"): "time",
    # pyarrow.time64("us"): "time",
    # pyarrow.timestamp(
    #     "us"
    # ): "timestamp",  # Note: becomes timestamptz if timezone specified
    # # Duration/Interval
    # pyarrow.duration("us"): "interval",
    # # Decimal Types
    # pyarrow.decimal128(precision=38, scale=9): "numeric",
    # pyarrow.decimal256(precision=76, scale=18): "numeric",
}


def arrow_field_to_pg_type(field: Union[pyarrow.Field, pyarrow.DataType]) -> str:
    """Get the PostgreSQL type name for a given PyArrow field.

    Parameters
    ----------
    field : pyarrow.Field
        The PyArrow field containing type and metadata information

    Returns
    -------
    str
        The corresponding PostgreSQL type name

    Raises
    ------
    ValueError
        If the field's type is not supported

    Examples
    --------
    >>> import pyarrow as pa
    >>> field = pyarrow.field('timestamp_col', pyarrow.timestamp('us', tz='UTC'))
    >>> get_pg_type(field)
    'timestamptz'
    >>> field = pyarrow.field('int_list', pyarrow.list_(pyarrow.int32()))
    >>> get_pg_type(field)
    'int4 ARRAY'
    >>> field = pyarrow.field('dict_col', pyarrow.dictionary(pyarrow.int8(), pyarrow.string()))
    >>> get_pg_type(field)
    'text'
    """

    def _resolve_type(
        arrow_type: Union[pyarrow.Field, pyarrow.DataType]
    ) -> Union[pyarrow.Field, pyarrow.DataType, str]:
        """Internal helper to resolve type, handling nested cases."""
        # Handle list types
        if pyarrow.types.is_list(arrow_type):
            value_type = _resolve_type(arrow_type.value_type)
            return f"{value_type} ARRAY"

        # Handle fixed size list types
        if pyarrow.types.is_fixed_size_list(arrow_type):
            value_type = _resolve_type(arrow_type.value_type)
            return f"{value_type} ARRAY"

        # Handle large list types
        if pyarrow.types.is_large_list(arrow_type):
            value_type = _resolve_type(arrow_type.value_type)
            return f"{value_type} ARRAY"

        # TODO Consider adding support for these types, with testing.

        # # Handle dictionary types - use value type
        # if pyarrow.types.is_dictionary(arrow_type):
        #     return _resolve_type(arrow_type.value_type)

        # # Handle timestamp with timezone
        # if pyarrow.types.is_timestamp(arrow_type) and arrow_type.tz is not None:
        #     return "timestamptz"

        # Look up base type
        pg_type = ARROW_TO_PG_TYPES.get(arrow_type)
        if pg_type is None:
            raise ValueError(f"Unsupported PyArrow type: {arrow_type}")

        # TODO Consider adding support for these types, with testing.

        # # Special handling for time types with different units
        # if pyarrow.types.is_time(arrow_type):
        #     return "time"

        # # Special handling for timestamp types without timezone
        # if pyarrow.types.is_timestamp(arrow_type):
        #     return "timestamp"

        # # Special handling for duration/interval types with different units
        # if pyarrow.types.is_duration(arrow_type):
        #     return "interval"

        return pg_type

    return _resolve_type(field.type)


# Mapping between Arrow types and DuckDB column type names
ARROW_TO_DUCKDB_TYPES = {
    # Boolean
    pyarrow.bool_(): "BOOLEAN",
    # Integers
    pyarrow.int8(): "TINYINT",
    pyarrow.uint8(): "UTINYINT",
    pyarrow.int16(): "SMALLINT",
    pyarrow.uint16(): "USMALLINT",
    pyarrow.int32(): "INTEGER",
    pyarrow.uint32(): "UINTEGER",
    pyarrow.int64(): "BIGINT",
    pyarrow.uint64(): "UBIGINT",
    # Floating point
    # pyarrow.float16(): "REAL",  # Note: gets converted to float32 internally
    pyarrow.float32(): "REAL",
    pyarrow.float64(): "DOUBLE",
    # Decimal
    pyarrow.decimal128(precision=38, scale=9): "DECIMAL",
    pyarrow.decimal256(precision=76, scale=38): "DECIMAL",
    # String types
    pyarrow.string(): "VARCHAR",
    pyarrow.large_string(): "VARCHAR",
    # TODO Consider adding support for these types, with testing.
    # # Binary
    # pyarrow.binary(): 'BLOB',
    # pyarrow.large_binary(): 'BLOB',
    # # Temporal types
    # pyarrow.date32(): 'DATE',
    # pyarrow.date64(): 'DATE',
    # pyarrow.time32('s'): 'TIME',
    # pyarrow.time32('ms'): 'TIME',
    # pyarrow.time64('us'): 'TIME',
    # pyarrow.time64('ns'): 'TIME',
    # pyarrow.timestamp('s'): 'TIMESTAMP',
    # pyarrow.timestamp('ms'): 'TIMESTAMP',
    # pyarrow.timestamp('us'): 'TIMESTAMP',
    # pyarrow.timestamp('ns'): 'TIMESTAMP',
    # pyarrow.timestamp('us', tz='UTC'): 'TIMESTAMP WITH TIME ZONE',
    # # Interval/Duration
    # pyarrow.duration('s'): 'INTERVAL',
    # pyarrow.duration('ms'): 'INTERVAL',
    # pyarrow.duration('us'): 'INTERVAL',
    # pyarrow.duration('ns'): 'INTERVAL',
}


def arrow_field_to_duckdb_type(field: Union[pyarrow.Field, pyarrow.DataType]) -> str:
    """Get the DuckDB type name for a given PyArrow field.

    Parameters
    ----------
    field : Union[pyarrow.Field, pyarrow.DataType]
        The PyArrow field or type to convert

    Returns
    -------
    str
        The corresponding DuckDB type name

    Raises
    ------
    ValueError
        If the field's type is not supported or cannot be mapped

    Examples
    --------
    >>> import pyarrow as pa
    >>> get_duckdb_type(pyarrow.int32())
    'INTEGER'
    >>> get_duckdb_type(pyarrow.timestamp('us', tz='UTC'))
    'TIMESTAMP WITH TIME ZONE'
    >>> get_duckdb_type(pyarrow.list_(pyarrow.int32()))
    'INTEGER[]'
    >>> struct_type = pyarrow.struct([('x', pyarrow.int32()), ('y', pyarrow.string())])
    >>> get_duckdb_type(struct_type)
    'STRUCT(x INTEGER, y VARCHAR)'
    """

    def _resolve_type(
        arrow_type: Union[pyarrow.Field, pyarrow.DataType]
    ) -> Union[pyarrow.Field, pyarrow.DataType, str]:
        """Internal helper to resolve type, handling nested cases."""
        # Handle decimal types with custom precision/scale
        if pyarrow.types.is_decimal(arrow_type):
            return f"DECIMAL({arrow_type.precision}, {arrow_type.scale})"

        # Handle list types (including large lists and fixed size lists)
        if (
            pyarrow.types.is_list(arrow_type)
            or pyarrow.types.is_large_list(arrow_type)
            or pyarrow.types.is_fixed_size_list(arrow_type)
        ):
            value_type = _resolve_type(arrow_type.value_type)
            return f"{value_type}[]"

        # TODO Consider adding support for these types, with testing.

        # # Handle fixed size binary
        # if pyarrow.types.is_fixed_size_binary(arrow_type):
        #     return f'BLOB({arrow_type.byte_width})'

        # # Handle dictionary types - use value type
        # if pyarrow.types.is_dictionary(arrow_type):
        #     return _resolve_type(arrow_type.value_type)

        # # Handle timestamp with timezone
        # if pyarrow.types.is_timestamp(arrow_type):
        #     if arrow_type.tz is not None:
        #         return 'TIMESTAMP WITH TIME ZONE'
        #     return 'TIMESTAMP'

        # # Handle struct types
        # if pyarrow.types.is_struct(arrow_type):
        #     fields = []
        #     for field in arrow_type:
        #         field_type = _resolve_type(field.type)
        #         fields.append(f'{field.name} {field_type}')
        #     return f'STRUCT({", ".join(fields)})'

        # # Handle map types
        # if pyarrow.types.is_map(arrow_type):
        #     key_type = _resolve_type(arrow_type.key_type)
        #     item_type = _resolve_type(arrow_type.item_type)
        #     return f'MAP({key_type}, {item_type})'

        # Look up base type
        duckdb_type = ARROW_TO_DUCKDB_TYPES.get(arrow_type)
        if duckdb_type is None:
            raise ValueError(f"Unsupported PyArrow type: {arrow_type}")
        return duckdb_type

    arrow_type = field.type if isinstance(field, pyarrow.Field) else field
    return _resolve_type(arrow_type)


ARROW_TO_SQLITE_TYPES: dict[pyarrow.Field, str] = {
    # Boolean - stored as INTEGER
    pyarrow.bool_(): "INTEGER",
    # Integers - all stored as INTEGER
    pyarrow.int8(): "INTEGER",
    pyarrow.uint8(): "INTEGER",
    pyarrow.int16(): "INTEGER",
    pyarrow.uint16(): "INTEGER",
    pyarrow.int32(): "INTEGER",
    pyarrow.uint32(): "INTEGER",
    pyarrow.int64(): "INTEGER",
    # pyarrow.uint64() not supported, may exceed SQLite INTEGER range
    # Floating point - stored as REAL
    # pyarrow.float16(): "REAL",
    pyarrow.float32(): "REAL",
    pyarrow.float64(): "REAL",
    # Decimal - stored as TEXT to preserve precision
    pyarrow.decimal128(precision=38, scale=9): "TEXT",
    pyarrow.decimal256(precision=76, scale=38): "TEXT",
    # String types - stored as TEXT
    pyarrow.string(): "TEXT",
    pyarrow.large_string(): "TEXT",
    # TODO Consider adding support for these types, with testing.
    # # Binary - stored as BLOB
    # pyarrow.binary(): 'BLOB',
    # pyarrow.large_binary(): 'BLOB',
    # pyarrow.fixed_size_binary_type(32): 'BLOB',
    # # Temporal types - stored as TEXT or INTEGER
    # pyarrow.date32(): 'TEXT',  # ISO8601 date string
    # pyarrow.date64(): 'TEXT',  # ISO8601 date string
    # pyarrow.time32('s'): 'TEXT',  # ISO8601 time string
    # pyarrow.time64('us'): 'TEXT',  # ISO8601 time string
    # pyarrow.timestamp('s'): 'INTEGER',  # Unix timestamp
    # pyarrow.timestamp('ms'): 'INTEGER',  # Unix timestamp
    # pyarrow.timestamp('us'): 'INTEGER',  # Unix timestamp
    # pyarrow.timestamp('ns'): 'INTEGER',  # Unix timestamp
    # pyarrow.timestamp('us', tz='UTC'): 'TEXT',  # ISO8601 timestamp with TZ
    # # Duration/Interval - stored as INTEGER (microseconds) or TEXT
    # pyarrow.duration('s'): 'INTEGER',
    # pyarrow.duration('ms'): 'INTEGER',
    # pyarrow.duration('us'): 'INTEGER',
    # pyarrow.duration('ns'): 'INTEGER'
}


def arrow_field_to_sqlite_type(field: Union[pyarrow.Field, pyarrow.DataType]) -> str:
    """Get the SQLite type name for a given PyArrow field.

    Parameters
    ----------
    field : Union[pyarrow.Field, pyarrow.DataType]
        The PyArrow field or type to convert

    Returns
    -------
    str
        The corresponding SQLite type name

    Raises
    ------
    ValueError
        If the field's type is not supported or cannot be mapped

    Examples
    --------
    >>> import pyarrow as pa
    >>> get_sqlite_type(pyarrow.int32())
    'INTEGER'
    >>> get_sqlite_type(pyarrow.string())
    'TEXT'
    >>> get_sqlite_type(pyarrow.timestamp('us', tz='UTC'))
    'TEXT'
    >>> get_sqlite_type(pyarrow.list_(pyarrow.int32()))
    'TEXT'  # JSON encoded array
    """

    def _resolve_type(arrow_type: pyarrow.DataType) -> str:
        # TODO Consider adding support for these, with tests.

        # Handle dictionary types - use value type
        # if pyarrow.types.is_dictionary(arrow_type):
        #     return _resolve_type(arrow_type.value_type)

        # Handle timestamp with timezone - store as TEXT
        # if pyarrow.types.is_timestamp(arrow_type) and arrow_type.tz is not None:
        #     return "TEXT"

        # Handle nested types (lists, structs, maps) - store as JSON TEXT
        # if (
        #     pyarrow.types.is_list(arrow_type)
        #     or pyarrow.types.is_struct(arrow_type)
        #     or pyarrow.types.is_map(arrow_type)
        #     or pyarrow.types.is_fixed_size_list(arrow_type)
        #     or pyarrow.types.is_large_list(arrow_type)
        # ):
        #     return "TEXT"  # JSON encoded

        # Look up base type
        sqlite_type = ARROW_TO_SQLITE_TYPES.get(arrow_type)
        if sqlite_type is None:
            raise ValueError(f"Unsupported PyArrow type: {arrow_type}")
        return sqlite_type

    arrow_type = field.type if isinstance(field, pyarrow.Field) else field
    return _resolve_type(arrow_type)


DIALECT_TO_TYPE_CONVERTER: dict[
    DIALECTS, Callable[[Union[pyarrow.Field, pyarrow.DataType]], str]
] = {
    "duckdb": arrow_field_to_duckdb_type,
    "sqlite": arrow_field_to_sqlite_type,
    "postgresql": arrow_field_to_pg_type,
}


def arrow_schema_to_column_defns(
    schema: pyarrow.Schema, dialect: DIALECTS
) -> dict[str, str]:
    """
    Given Arrow schema, return mapping of column names to type definitions.

    Example output: {'x': 'INTEGER NOT NULL'}
    """
    # Check for possible column name collisions when converted to lower case
    all_names = [field.name.lower() for field in schema]
    if len(all_names) != len(set(all_names)):
        raise ValueError("Column names must be unique when converted to lower case.")
    columns = {}
    converter = DIALECT_TO_TYPE_CONVERTER[dialect]
    for field in schema:
        is_safe_identifier(field.name, COLUMN_NAME_PATTERN, allow_reserved_words=True)
        sql_type = converter(field)
        nullable = "NULL" if field.nullable else "NOT NULL"
        columns[field.name.lower()] = f"{sql_type} {nullable}"
    return columns


def arrow_schema_to_create_table(
    schema: pyarrow.Schema, table_name: str, dialect: DIALECTS
) -> str:
    "Construct the CREATE TABLE statement"
    columns = arrow_schema_to_column_defns(schema, dialect)
    return (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" ('
        + ",\n ".join(f'"{name}" {type_}' for name, type_ in columns.items())
        + ")"
    )


def is_safe_identifier(
    identifier: str,
    pattern: re.Pattern[str],
    allow_reserved_words: bool = True,
) -> bool:
    if len(identifier) > 63:
        raise ValueError(
            f'Invalid SQL identifier "{identifier}": max character number is 63'
        )

    if not allow_reserved_words and identifier.lower() in RESERVED_WORDS:
        raise ValueError(
            f'Reserved SQL keywords are not allowed in identifiers, "{identifier}"'
        )

    if pattern.match(identifier) is None:
        raise ValueError(f'Malformed SQL identifier "{identifier}"')

    if match := FORBIDDEN_CHARACTERS.search(identifier):
        raise ValueError(
            f'Invalid SQL identifier "{identifier}" '
            f"contains forbidden character(s): {', '.join(match.groups())}"
        )

    return True
