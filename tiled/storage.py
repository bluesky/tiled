import dataclasses
import functools
import os
from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional, Union
from urllib.parse import urlparse, urlunparse

import sqlalchemy.pool

if TYPE_CHECKING:
    import adbc_driver_manager.dbapi

from .utils import ensure_uri, path_from_uri, sanitize_uri

__all__ = [
    "EmbeddedSQLStorage",
    "RemoteSQLStorage",
    "FileStorage",
    "SQLStorage",
    "Storage",
    "get_storage",
    "parse_storage",
]


@dataclasses.dataclass(frozen=True)
class Storage:
    "Base class for representing storage location"
    uri: str

    def __post_init__(self):
        object.__setattr__(self, "uri", ensure_uri(self.uri))


@dataclasses.dataclass(frozen=True)
class FileStorage(Storage):
    "Filesystem storage location"

    @functools.cached_property
    def path(self):
        return path_from_uri(self.uri)


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


@dataclasses.dataclass(frozen=True)
class SQLStorage(Storage):
    "General purpose SQL database storage with connection pooling"

    pool_size: int = 5
    max_overflow: int = 10

    def __post_init__(self):
        # Ensure pool_size and max_overflow are set to a default if not specified
        if self.dialect == "duckdb":
            # DuckDB does not support pooling, so we use StaticPool
            object.__setattr__(self, "pool_size", 1)
            object.__setattr__(self, "max_overflow", 0)
        else:
            if self.pool_size is None:
                object.__setattr__(self, "pool_size", 5)
            if self.max_overflow is None:
                object.__setattr__(self, "max_overflow", 10)

        super().__post_init__()

    @abstractmethod
    def create_adbc_connection(self) -> "adbc_driver_manager.dbapi.Connection":
        "Create a connection to the database."

        raise NotImplementedError("Subclasses must implement this method.")

    @property
    def dialect(self) -> str:
        "The database dialect (e.g. 'postgresql', 'sqlite', or 'duckdb')."
        return urlparse(self.uri).scheme

    @functools.cached_property
    def _adbc_connection(self) -> "adbc_driver_manager.dbapi.Connection":
        "A persistent connection to the database."
        return self.create_adbc_connection()

    @functools.cached_property
    def _connection_pool(self) -> "sqlalchemy.pool.QueuePool":
        creator = self._adbc_connection.adbc_clone
        if (self.dialect == "duckdb") or (":memory:" in self.uri):
            return sqlalchemy.pool.StaticPool(creator)
        else:
            return sqlalchemy.pool.QueuePool(
                creator, pool_size=self.pool_size, max_overflow=self.max_overflow
            )

    def connect(self) -> "adbc_driver_manager.dbapi.Connection":
        "Get a connection from the pool."
        return self._connection_pool.connect()

    def dispose(self) -> None:
        "Close all connections and dispose of the connection pool."
        self._connection_pool.dispose()
        self._adbc_connection.close()


@dataclasses.dataclass(frozen=True)
class EmbeddedSQLStorage(SQLStorage):
    "File-based SQL database storage location"

    def create_adbc_connection(self) -> "adbc_driver_manager.dbapi.Connection":
        filepath = _ensure_writable_location(self.uri)

        if self.uri.startswith("duckdb:"):
            import adbc_driver_duckdb.dbapi

            return adbc_driver_duckdb.dbapi.connect(str(filepath))

        elif self.uri.startswith("sqlite:"):
            import adbc_driver_sqlite.dbapi

            return adbc_driver_sqlite.dbapi.connect(str(filepath))

        else:
            raise ValueError(
                f"Unsupported URI scheme {self.uri}: use 'duckdb:' or 'sqlite:'"
            )


@dataclasses.dataclass(frozen=True)
class RemoteSQLStorage(SQLStorage):
    "Authenticated server-based SQL database storage location"
    username: Optional[str] = None
    password: Optional[str] = None

    def __post_init__(self):
        # Ensure the URI is sanitized and credentials are stored separately
        if not self.uri.startswith("postgresql:"):
            raise ValueError(f"Unsupported URI scheme {self.uri}: use 'postgresql:'")

        uri, username, password = sanitize_uri(self.uri)
        if (username is not None) or (password is not None):
            if (self.username is not None) or (self.password is not None):
                raise ValueError(
                    "Credentials passed both in URI and in username/password fields."
                )
            object.__setattr__(self, "uri", uri)
            object.__setattr__(self, "username", username)
            object.__setattr__(self, "password", password)

        super().__post_init__()

    def __repr__(self):
        return f"SQLStorage(uri={self.uri!r}, username={self.username!r})"

    @functools.cached_property
    def authenticated_uri(self):
        parsed_uri = urlparse(self.uri)
        components = (
            parsed_uri.scheme,
            f"{self.username}:{self.password}@{parsed_uri.netloc}",
            parsed_uri.path,
            parsed_uri.params,
            parsed_uri.query,
            parsed_uri.fragment,
        )

        return urlunparse(components)

    def create_adbc_connection(self) -> "adbc_driver_manager.dbapi.Connection":
        import adbc_driver_postgresql.dbapi

        return adbc_driver_postgresql.dbapi.connect(self.authenticated_uri)


def parse_storage(
    item: Union[Path, str],
    *,
    pool_size: Optional[int] = None,
    max_overflow: Optional[int] = None,
) -> Storage:
    "Create a Storage object from a URI or Path."
    item = ensure_uri(item)
    scheme = urlparse(item).scheme
    if scheme == "file":
        result = FileStorage(item)
    elif scheme == "postgresql":
        result = RemoteSQLStorage(item, pool_size=pool_size, max_overflow=max_overflow)
    elif scheme in {"sqlite", "duckdb"}:
        result = EmbeddedSQLStorage(
            item, pool_size=pool_size, max_overflow=max_overflow
        )
    else:
        raise ValueError(f"writable_storage item {item} has unrecognized scheme")
    return result


# This global registry enables looking up Storage via URI, primarily for the
# purpose of obtaining credentials, which are not stored in the catalog
# database.
_STORAGE: Dict[str, Storage] = {}


def register_storage(storage: Storage) -> None:
    "Stash Storage for later lookup by URI."
    _STORAGE[storage.uri] = storage


def get_storage(uri: str) -> Storage:
    "Look up Storage by URI."
    return _STORAGE[uri]
