import copy
import hashlib
import os
import re
import secrets
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
)

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
from .utils import init_adapter_from_catalog

if TYPE_CHECKING:
    import adbc_driver_duckdb.dbapi
    import adbc_driver_postgresql.dbapi
    import adbc_driver_sqlite.dbapi
DIALECTS = Literal["postgresql", "sqlite", "duckdb"]


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
    ) -> None:
        """
        Construct the SQLAdapter object.
        Parameters
        ----------
        data_uri : the uri of the database, starting either with "duckdb://" or "postgresql://"
        structure : the structure of the data; structure is not optional for sql database
        metadata : the optional metadata of the data.
        specs : the specs.
        """
        self.uri = data_uri

        self.conn = create_connection(self.uri)
        self.cur = self.conn.cursor()

        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])
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

        conn = create_connection(data_uri)
        dialect, _ = data_uri.split(":", 1)
        schema_new = schema.insert(0, pyarrow.field("dataset_id", pyarrow.int64()))
        create_table_statement = arrow_schema_to_create_table(
            schema_new, table_name, cast(DIALECTS, dialect)
        )

        create_index_statement = (
            "CREATE INDEX IF NOT EXISTS dataset_id_index "
            f"ON {table_name}(dataset_id)"
        )
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
        Function to read a batch of data from a given partition.
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


def create_connection(
    uri: str,
) -> Union[
    adbc_driver_duckdb.dbapi.Connection,
    adbc_driver_sqlite.dbapi.Connection,
    adbc_driver_postgresql.dbapi.Connection,
]:
    """
    Function to create an adbc connection of type duckdb , sqlite or postgresql.
    Parameters
    ----------
    uri : the uri which is used to create a connection
    Returns
    -------
    Returns a connection of type duckdb , sqlite or postgresql.
    """
    if uri.startswith("duckdb:"):
        import adbc_driver_duckdb.dbapi

        filepath = _ensure_writable_location(uri)
        conn = adbc_driver_duckdb.dbapi.connect(str(filepath))
    elif uri.startswith("sqlite:"):
        import adbc_driver_sqlite.dbapi

        filepath = _ensure_writable_location(uri)
        conn = adbc_driver_sqlite.dbapi.connect(str(filepath))
    elif uri.startswith("postgresql:"):
        import adbc_driver_postgresql.dbapi

        conn = adbc_driver_postgresql.dbapi.connect(uri)
    else:
        raise ValueError(
            "The database uri must start with `duckdb:`, `sqlite:`, or `postgresql:`"
        )
    return conn


def _ensure_writable_location(uri: str) -> Path:
    "Ensure path is writable to avoid a confusing error message from driver."
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
    return filepath


def add_dataset_column(table: pyarrow.Table, dataset_id: int) -> pyarrow.Table:
    column = dataset_id * numpy.ones(len(table), dtype=numpy.int64)
    return table.add_column(0, pyarrow.field("dataset_id", pyarrow.int64()), [column])


# Mapping between Arrow types and PostgreSQL column type name.
ARROW_TO_PG_TYPES: dict[pyarrow.Field, str] = {
    # Boolean
    pyarrow.bool_(): "bool",
    # Integers
    pyarrow.int8(): "int2",
    pyarrow.uint8(): "int2",
    pyarrow.int16(): "int2",
    pyarrow.uint16(): "int4",
    pyarrow.int32(): "int4",
    pyarrow.uint32(): "int8",
    pyarrow.int64(): "int8",
    pyarrow.uint64(): "int8",
    # Floating Point
    pyarrow.float16(): "float4",
    pyarrow.float32(): "float4",
    pyarrow.float64(): "float8",
    # String Types
    pyarrow.string(): "text",
    pyarrow.large_string(): "text",
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
        for pa_type, pg_type in ARROW_TO_PG_TYPES.items():
            if arrow_type == pa_type:
                return pg_type

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

        raise ValueError(f"Unsupported PyArrow type: {arrow_type}")

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
    pyarrow.float16(): "REAL",  # Note: gets converted to float32 internally
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
        for pa_type, duck_type in ARROW_TO_DUCKDB_TYPES.items():
            if arrow_type == pa_type:
                return duck_type

        raise ValueError(f"Unsupported PyArrow type: {arrow_type}")

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
    pyarrow.uint64(): "INTEGER",  # Note: may exceed SQLite INTEGER range
    # Floating point - stored as REAL
    pyarrow.float16(): "REAL",
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
        # Handle dictionary types - use value type
        if pyarrow.types.is_dictionary(arrow_type):
            return _resolve_type(arrow_type.value_type)

        # Handle timestamp with timezone - store as TEXT
        if pyarrow.types.is_timestamp(arrow_type) and arrow_type.tz is not None:
            return "TEXT"

        # Handle nested types (lists, structs, maps) - store as JSON TEXT
        if (
            pyarrow.types.is_list(arrow_type)
            or pyarrow.types.is_struct(arrow_type)
            or pyarrow.types.is_map(arrow_type)
            or pyarrow.types.is_fixed_size_list(arrow_type)
            or pyarrow.types.is_large_list(arrow_type)
        ):
            return "TEXT"  # JSON encoded

        # Look up base type
        for pa_type, sqlite_type in ARROW_TO_SQLITE_TYPES.items():
            if arrow_type == pa_type:
                return sqlite_type

        raise ValueError(f"Unsupported PyArrow type: {arrow_type}")

    arrow_type = field.type if isinstance(field, pyarrow.Field) else field
    return _resolve_type(arrow_type)


DIALECT_TO_TYPE_CONVERTER: dict[
    DIALECTS, Callable[[Union[pyarrow.Field, pyarrow.DataType]], str]
] = {
    "duckdb": arrow_field_to_duckdb_type,
    "sqlite": arrow_field_to_sqlite_type,
    "postgresql": arrow_field_to_pg_type,
}


def arrow_schema_to_create_table(
    schema: pyarrow.Schema, table_name: str, dialect: DIALECTS
) -> str:
    # Build column definitions
    columns = []

    converter = DIALECT_TO_TYPE_CONVERTER[dialect]
    for field in schema:
        sql_type = converter(field)
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
