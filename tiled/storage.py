import dataclasses
import functools
import os
from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Literal, Optional, Union
from urllib.parse import urlparse, urlunparse

import sqlalchemy.pool

from .utils import ensure_uri, path_from_uri, sanitize_uri

if TYPE_CHECKING:
    import adbc_driver_manager.dbapi
    from obstore.store import AzureStore, GCSStore, LocalStore, S3Store

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


SUPPORTED_OBJECT_URI_SCHEMES = {"http", "https"}  # TODO: Add "s3", "gs", "azure", "az"


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

    def get_obstore_location(self, uri=None) -> "LocalStore":
        """Get an obstore.store.LocalStore instance rooted at specified URI.

        Parameters
        ----------
            uri: str, optional
        """

        if (uri is not None) and (not uri.startswith(self.uri)):
            raise ValueError(
                f"Requested URI {uri} is not within the base FileStorage URI {self.uri}"
            )

        from obstore.store import LocalStore

        directory = path_from_uri(uri)
        directory.mkdir(parents=True, exist_ok=True)

        return LocalStore(directory)


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
    """Bucket storage location for BLOBS

    The uri should include the bucket, but not the prefix within the bucket. This
    allows multiple ObjectStorage to point to different buckets with different
    credentials.

    Parameters
    ----------
        uri: str
            Base URI, including bucket, but without prefix
        provider: Literal["s3", "azure", "google"]
        bucket: Optional[str]
            Only required for s3 and google
        username: Optional[str]
        password: Optional[str]
        config: dict
            Additional configuration options passed to obstore store classes.
    """

    uri: str
    provider: Literal["s3", "azure", "google"]
    bucket: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    config: dict = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        base_uri, bucket_name, _ = self.parse_blob_uri(ensure_uri(self.uri))
        base_uri, username, password = sanitize_uri(base_uri)

        if (username is not None) or (password is not None):
            if (
                (self.username is not None)
                or (self.password is not None)
                or ("username" in self.config)
                or ("password" in self.config)
            ):
                raise ValueError(
                    "Credentials passed in URI and in username, password, or config fields."
                )
            object.__setattr__(self, "username", username)
            object.__setattr__(self, "password", password)

        object.__setattr__(self, "uri", base_uri)
        object.__setattr__(self, "bucket", bucket_name)

    @classmethod
    def parse_blob_uri(cls, uri: str) -> tuple[str, str]:
        """Split a blob URI into base URI, bucket name (optionally), and the prefix.

        For example, given 'http://example.com/bucket_name/path/to/blob',
        return ('http://example.com', 'bucket_name', 'path/to/blob').
        """

        # TODO: THIS NEEDS MORE WORK TO HANDLE S3, GCS, AZURE DIFFERENCES PROPERLY
        #       CURRENTLY ONLY HANDLES HTTP(S) STYLE URIS

        parsed_uri = urlparse(uri)
        full_path = parsed_uri.path  # includes bucket and the rest
        bucket_name, *blob_path = full_path.strip("/").split("/", 1)
        base_uri = f"{parsed_uri.scheme}://{parsed_uri.netloc}/{bucket_name}"

        return base_uri, bucket_name or None, "/".join(blob_path)

    def get_obstore_location(
        self, uri=None
    ) -> Union["S3Store", "AzureStore", "GCSStore"]:
        """Get an obstore.store instance rooted at specified URI.

        Parameters
        ----------
            uri: str, optional
                The URI to use as the root for the obstore location. If not specified, use the base URI of
                this ObjectStorage.

        Returns
        -------
            An instance of obstore.store.S3Store, obstore.store.AzureStore, or obstore.store.GCSStore,
            depending on the provider.
        """

        if (uri is not None) and (not uri.startswith(self.uri)):
            raise ValueError(
                f"Requested URI {uri} is not within the base ObjectStorage URI {self.uri}"
            )

        from obstore.store import AzureStore, GCSStore, S3Store

        # Build kwargs for the specific store class based on provider
        if self.provider == "s3":
            kwargs = {
                "endpoint": self.uri.split(self.bucket, -1)[0],
                "bucket": self.bucket,
            }
            if self.username is not None:
                kwargs["access_key_id"] = self.username
            if self.password is not None:
                kwargs["secret_access_key"] = self.password

        elif self.provider == "azure":
            kwargs = {"endpoint": self.uri}
            if self.username is not None:
                kwargs["client_id"] = self.username
            if self.password is not None:
                kwargs["client_secret"] = self.password

        elif self.provider == "google":
            kwargs = {"url": self.uri.split(self.bucket, -1)[0], "bucket": self.bucket}
            if self.password is not None:
                kwargs["service_account_key"] = self.password

        _class = {"s3": S3Store, "azure": AzureStore, "google": GCSStore}[self.provider]
        prefix = uri[len(self.uri) :].lstrip("/") if uri else None  # noqa: E203

        return _class(**kwargs, **self.config, prefix=prefix)


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
        if self.username is None and self.password is None:
            return self.uri

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


def get_storage(uri: str) -> Storage:
    "Look up Storage by URI."

    if urlparse(uri).scheme in SUPPORTED_OBJECT_URI_SCHEMES:
        uri, _, _ = ObjectStorage.parse_blob_uri(uri)

    return _STORAGE[uri]
