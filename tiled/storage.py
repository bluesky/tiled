import dataclasses
import functools
import os
import re
from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Literal, Optional, Tuple, Union
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
    "ObjectStorage",
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
class ObjectStorage(Storage):
    "Bucket storage location for BLOBS"

    uri: str
    provider: Literal["s3", "azure", "google"]
    config: dict

    def __post_init__(self):
        base_uri, _ = self.split_blob_uri(ensure_uri(self.uri))
        object.__setattr__(self, "uri", base_uri)

    @classmethod
    def split_blob_uri(cls, uri: str) -> tuple[str, str]:
        """Split a blob URI into base URI and blob path.

        For example, given 'http://example.com/bucket_name/path/to/blob',
        return ('http://example.com/bucket_name', 'path/to/blob').
        """
        parsed_uri = urlparse(uri)
        full_path = parsed_uri.path  # includes bucket and the rest
        bucket_name, blob_path = full_path.split("/", 1)
        base_uri = f"{parsed_uri.scheme}://{parsed_uri.netloc}/{bucket_name}"

        return base_uri, blob_path

    def get_object_store(self, prefix=None) -> "S3Store | AzureStore | GCSStore":
        """Get an object store instance based on the provider and config."""

        from obstore.azure import AzureStore
        from obstore.gcs import GCSStore
        from obstore.s3 import S3Store

        _class = {"s3": S3Store, "azure": AzureStore, "google": GCSStore}[self.provider]
        _uri_property = {"s3": "endpoint", "azure": "endpoint", "google": "url"}[
            self.provider
        ]

        return _class(**{_uri_property: self.uri}, **self.config, prefix=prefix)


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
        from .server.metrics import monitor_db_pool

        creator = self._adbc_connection.adbc_clone
        if (self.dialect == "duckdb") or (":memory:" in self.uri):
            pool = sqlalchemy.pool.StaticPool(creator)
        else:
            pool = sqlalchemy.pool.QueuePool(
                creator, pool_size=self.pool_size, max_overflow=self.max_overflow
            )
            monitor_db_pool(pool, self.uri)

        return pool

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
    item: Union[Path, str, dict],
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
) -> Storage:
    "Create a Storage object from a URI or Path."
    if isinstance(item, dict):
        result = ObjectStorage(
            uri=item["uri"],
            provider=item["provider"],
            config=item.get("config", {}),
        )
    else:
        item = ensure_uri(item)
        scheme = urlparse(item).scheme
        if scheme == "file":
            result = FileStorage(item)
        elif scheme == "postgresql":
            result = RemoteSQLStorage(
                item, pool_size=pool_size, max_overflow=max_overflow
            )
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


def get_storage(uri: str) -> Storage | Tuple[Storage, str]:
    parsed_uri = urlparse(uri)
    if parsed_uri.scheme == "file":
        return FileStorage(uri)
    elif parsed_uri.scheme in {"sqlite", "duckdb"}:
        return EmbeddedSQLStorage(uri)
    elif parsed_uri.scheme == "http":
        full_path = parsed_uri.path  # includes bucket and the rest
        bucket_name, blob_path = full_path.split("/", 1)

        base_url = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
        return ObjectStorage(base_url)

        # Split on the first single '/' that is not part of '://'
        match = re.match(r"([^:/]+://[^/]+|[^/]+)(/.*)?", uri)
        objstore = ObjectStorage(match.group(1)) if match else ObjectStorage(uri)
        path = (
            (match.group(2) if match and match.group(2) else "")
            .lstrip("/")
            .replace(objstore.config["bucket"] + "/", "")
        )
        return objstore, path
    else:
        "Look up Storage by URI."
        return _STORAGE[uri]
