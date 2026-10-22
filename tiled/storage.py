import dataclasses
import functools
import os
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    DefaultDict,
    Dict,
    Iterator,
    Literal,
    NamedTuple,
    Optional,
    Union,
)
from urllib.parse import quote, unquote, urlparse, urlunparse

import sqlalchemy.pool

from .utils import ensure_uri, path_from_uri, sanitize_uri

if TYPE_CHECKING:
    import adbc_driver_manager.dbapi
    from obstore.store import AzureStore, GCSStore, LocalStore, S3Store

__all__ = [
    "DirectoryContainer",
    "EmbeddedSQLStorage",
    "RemoteSQLStorage",
    "FileStorage",
    "SQLStorage",
    "ObjectStorage",
    "Storage",
    "acquire_storage",
    "get_storage",
    "parse_storage",
    "release_storage",
    "storage_registry_key",
    "strip_connection_parameters",
    "unregister_storage",
]


SUPPORTED_OBJECT_URI_SCHEMES = {"http", "https"}  # TODO: Add "s3", "gs", "azure", "az"

POSTGRESQL_SCHEMES = frozenset({"postgresql", "postgres"})

# libpq connection parameters that determine *which* server and database a
# connection reaches. Any other query parameter (connect_timeout, sslmode,
# target_session_attrs, application_name, ...) only tunes *how* we connect.
# https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
POSTGRESQL_LOCATION_QUERY_PARAMETERS = frozenset(
    {"host", "hostaddr", "port", "dbname", "service"}
)


def strip_storage_uri_query_on_write() -> bool:
    """Feature flag: write Asset URIs without connection-tuning parameters.

    See `strip_connection_parameters`. Read at call time, not import time,
    so it can be flipped by restarting with a new environment and so tests
    can toggle it.

    Rollout: first deploy code that matches on read by `storage_registry_key`
    (always on), then set TILED_STRIP_STORAGE_URI_QUERY_ON_WRITE=1 so new
    Assets are written in stripped form.
    """
    return bool(int(os.getenv("TILED_STRIP_STORAGE_URI_QUERY_ON_WRITE", "0") or "0"))


def _query_pairs(query: str) -> list[tuple[str, str]]:
    "Split a query string into (decoded key, raw pair) without re-encoding."
    return [(unquote(pair.split("=", 1)[0]), pair) for pair in query.split("&") if pair]


def strip_connection_parameters(uri: str) -> str:
    """Drop query parameters that only tune how we connect.

    For PostgreSQL URIs, remove parameters such as `?connect_timeout=5` or
    `?target_session_attrs=read-write`, keeping those that locate the server
    and database (see POSTGRESQL_LOCATION_QUERY_PARAMETERS). Kept parameters
    are preserved verbatim and in order, so a URI with nothing to strip is
    returned unchanged. Other schemes are returned unchanged.

    This is the form written to `Asset.data_uri` when the
    TILED_STRIP_STORAGE_URI_QUERY_ON_WRITE feature flag is on. It retains
    the hosts, which is informative for humans, but matching Assets to
    Storage does not depend on them; see `storage_registry_key`.
    """
    parsed = urlparse(uri)
    if parsed.scheme not in POSTGRESQL_SCHEMES or not parsed.query:
        return uri
    kept = [
        pair
        for key, pair in _query_pairs(parsed.query)
        if key in POSTGRESQL_LOCATION_QUERY_PARAMETERS
    ]
    return urlunparse(parsed._replace(query="&".join(kept)))


def storage_registry_key(uri: str) -> str:
    """Return the key used to match an Asset's `data_uri` to a Storage.

    For PostgreSQL, this is the database name alone, so matching is
    insensitive to everything about *how* the database is reached: host
    order, cluster membership, a switch to a proxy or pooler endpoint
    (HAProxy, pgbouncer), ports, credentials, and connection-tuning query
    parameters. This keeps Assets written under one configuration
    resolvable after that configuration changes. The database name is taken
    from a `dbname` query parameter if present (as libpq does), else from
    the path.

    The trade-off is that two configured storages with the same database
    name on different servers would be indistinguishable;
    `register_storage` refuses that configuration.

    Other schemes are returned unchanged.
    """
    parsed = urlparse(uri)
    if parsed.scheme not in POSTGRESQL_SCHEMES:
        return uri
    dbname = unquote(parsed.path.lstrip("/"))
    for key, pair in _query_pairs(parsed.query):
        if key == "dbname":
            dbname = unquote(pair.split("=", 1)[1]) if "=" in pair else ""
    return f"postgresql:///{quote(dbname, safe='')}"


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
    def parse_blob_uri(cls, uri: str) -> tuple[str, str, str]:
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
        """Close all connections and dispose of the connection pool.

        Safe to call more than once, and does not open a connection that
        was never opened. Afterward the Storage remains usable: the next
        `connect()` opens a fresh pool.
        """
        # Pop the cached_property values so they are rebuilt on next use.
        pool = self.__dict__.pop("_connection_pool", None)
        connection = self.__dict__.pop("_adbc_connection", None)
        if pool is not None:
            pool.dispose()
        if connection is not None:
            connection.close()


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


# One SQLStorage instance per distinct configuration per process. Each
# instance owns a connection pool (and a template connection), so sharing
# instances shares pools: many catalog mounts configured with the same
# storage use one pool rather than one each.
_SQL_STORAGE_INSTANCES: Dict["SQLStorage", "SQLStorage"] = {}


def parse_storage(
    item: Union[Path, str, dict],
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
) -> Storage:
    """Create a Storage object from a URI or Path.

    SQL storage is shared: if an equal SQLStorage (same URI, credentials,
    and pool settings) has already been created in this process, that
    instance---and so its connection pool---is returned.
    """
    result = _parse_storage(item, pool_size=pool_size, max_overflow=max_overflow)
    if isinstance(result, SQLStorage):
        result = _SQL_STORAGE_INSTANCES.setdefault(result, result)
    return result


def _parse_storage(
    item: Union[Path, str, dict],
    *,
    pool_size: int,
    max_overflow: int,
) -> Storage:
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
# database. It is keyed by `storage_registry_key`, so for PostgreSQL lookups
# depend only on the database name, not on how it is reached.
_STORAGE: Dict[str, Storage] = {}


class AmbiguousStorage(ValueError):
    "Two configured storages are indistinguishable by `storage_registry_key`."


def _differing_fields(a: Storage, b: Storage) -> list[str]:
    "Names (never values: they may be credentials) of fields that differ."
    if type(a) is not type(b):
        return ["type"]
    return [
        field.name
        for field in dataclasses.fields(a)
        if getattr(a, field.name) != getattr(b, field.name)
    ]


def register_storage(storage: Storage) -> None:
    """Stash Storage for later lookup by URI.

    Each storage location (for PostgreSQL, each database name; see
    `storage_registry_key`) may have only one configuration per process.
    Re-registering an equal Storage (e.g. one per catalog mount) is allowed.
    Registering a different one for the same location raises
    `AmbiguousStorage`: Assets are matched to Storage by location alone, so
    there would be no way to choose between them, and a mismatch would
    silently use the other configuration's credentials and pool.
    """
    key = storage_registry_key(storage.uri)
    existing = _STORAGE.get(key)
    if (existing is not None) and (existing != storage):
        raise AmbiguousStorage(
            f"Storage {storage!r} conflicts with already-configured storage "
            f"{existing!r}: they refer to the same storage location (for "
            "PostgreSQL, the same database name) but are configured "
            f"differently (differing: {', '.join(_differing_fields(storage, existing))})."
            " Configure every reference to a given storage location identically "
            "(same URI, hosts, query parameters, credentials, and pool settings)."
        )
    _STORAGE[key] = storage


def unregister_storage(storage: Storage) -> None:
    "Remove Storage from the registry (call after dispose to release all references)."
    _STORAGE.pop(storage_registry_key(storage.uri), None)


# Number of active users (e.g. started catalog Contexts) per storage location,
# keyed by `storage_registry_key`. Since the registry admits one
# configuration per location, this counts users of one Storage.
_STORAGE_REFCOUNTS: DefaultDict[str, int] = defaultdict(int)


def acquire_storage(storage: Storage) -> Storage:
    """Register Storage and take a reference to it; pair with `release_storage`.

    Returns the canonical instance, which callers should use in place of
    the one passed in: for SQL storage, an equal instance may already be
    shared (see `parse_storage`), including one that a previous release
    disposed and that is being acquired again.
    """
    if isinstance(storage, SQLStorage):
        storage = _SQL_STORAGE_INSTANCES.setdefault(storage, storage)
    register_storage(storage)
    key = storage_registry_key(storage.uri)
    _STORAGE_REFCOUNTS[key] += 1
    return storage


def release_storage(storage: Storage) -> None:
    """Release a reference taken by `acquire_storage`.

    When the last reference to a storage location is released, its
    connections are closed and it is unregistered and (for SQL storage)
    dropped from the shared-instance cache.
    """
    key = storage_registry_key(storage.uri)
    _STORAGE_REFCOUNTS[key] -= 1
    if _STORAGE_REFCOUNTS[key] > 0:
        return
    # Last reference (or a release without a matching acquire): drop the
    # entry rather than leave a zero or negative count behind.
    del _STORAGE_REFCOUNTS[key]
    registered = _STORAGE.pop(key, storage)
    if isinstance(registered, SQLStorage):
        registered.dispose()
        if _SQL_STORAGE_INSTANCES.get(registered) is registered:
            del _SQL_STORAGE_INSTANCES[registered]


def get_storage(uri: str) -> Storage:
    """Look up Storage by URI.

    The URI is normalized with `storage_registry_key`, so an Asset written
    under an earlier configuration (different hosts, host order, ports, or
    connection-tuning query parameters) still matches the configured
    Storage for the same database.
    """

    if urlparse(uri).scheme in SUPPORTED_OBJECT_URI_SCHEMES:
        uri, _, _ = ObjectStorage.parse_blob_uri(uri)

    return _STORAGE[storage_registry_key(uri)]


def size_from_uri(data_uri: str) -> int:
    """Return the byte length of the asset at `data_uri`.

    Dispatches on URI scheme: `file://` URIs are stat'd on the local
    filesystem; object-store URIs (those in `SUPPORTED_OBJECT_URI_SCHEMES`)
    issue a HEAD via obstore. Raises `ValueError` for any other scheme.
    Underlying I/O errors (`FileNotFoundError`, network errors, etc.)
    propagate; callers that want best-effort behavior should catch them at
    the call site.
    """
    scheme = urlparse(data_uri).scheme
    if scheme == "file":
        return path_from_uri(data_uri).stat().st_size
    if scheme in SUPPORTED_OBJECT_URI_SCHEMES:
        storage = get_storage(data_uri)
        _, _, path = ObjectStorage.parse_blob_uri(data_uri)
        store = storage.get_obstore_location()
        return int(store.head(path)["size"])
    raise ValueError(
        f"Cannot stat URI with unsupported scheme {scheme!r}: {data_uri!r}"
    )


def mtime_from_uri(data_uri: str) -> float:
    """Return the last-modified time of the asset at `data_uri`.

    The time is a POSIX timestamp (seconds since the epoch).

    Dispatches on URI scheme: `file://` URIs are stat'd on the local
    filesystem; object-store URIs (those in `SUPPORTED_OBJECT_URI_SCHEMES`)
    issue a HEAD via obstore. Raises `ValueError` for any other scheme.
    Underlying I/O errors (`FileNotFoundError`, network errors, etc.)
    propagate; callers that want best-effort behavior should catch them at
    the call site.
    """
    scheme = urlparse(data_uri).scheme
    if scheme == "file":
        return path_from_uri(data_uri).stat().st_mtime
    if scheme in SUPPORTED_OBJECT_URI_SCHEMES:
        storage = get_storage(data_uri)
        _, _, path = ObjectStorage.parse_blob_uri(data_uri)
        store = storage.get_obstore_location()
        return store.head(path)["last_modified"].timestamp()
    raise ValueError(
        f"Cannot stat URI with unsupported scheme {scheme!r}: {data_uri!r}"
    )


class UriStat(NamedTuple):
    """Result of :func:`stat_uri`.

    A `NamedTuple` so callers can unpack it (`is_directory, size = stat_uri(...)`)
    or access fields by name, while leaving room to add more fields later without
    breaking existing unpacking.
    """

    is_directory: bool
    size: Optional[int]


def stat_uri(data_uri: str) -> UriStat:
    """Best-effort :class:`UriStat` (`is_directory`, `size`) for `data_uri`.

    Populates the advisory `is_directory` and `size` fields of an
    :class:`~tiled.structures.data_source.Asset`. A `file://` directory reports
    `(True, None)`; anything else reports `(False, <size>)` using
    :func:`size_from_uri`. Because these fields are advisory -- and the caller
    may not even have local access to the asset -- any failure to inspect the
    asset yields `(False, None)` rather than raising.
    """
    try:
        if urlparse(data_uri).scheme == "file" and path_from_uri(data_uri).is_dir():
            return UriStat(is_directory=True, size=None)
        return UriStat(is_directory=False, size=size_from_uri(data_uri))
    except Exception:
        return UriStat(is_directory=False, size=None)


class DirectoryContainer(Mapping[str, bytes]):
    """A storage container for byte-arrays representing Awkward Array buffers

    Each buffer is stored as a separate file in a given directory, with the
    filename corresponding to the form key.
    """

    def __init__(self, directory: Path):
        self.directory = directory

    def __getitem__(self, key: str) -> bytes:
        with open(self.directory / key, "rb") as file:
            return file.read()

    def __setitem__(self, key: str, value: bytes) -> None:
        with open(self.directory / key, "wb") as file:
            file.write(value)

    def __delitem__(self, key: str) -> None:
        (self.directory / key).unlink(missing_ok=True)

    def __iter__(self) -> Iterator[str]:
        yield from (p.name for p in self.directory.iterdir())

    def __len__(self) -> int:
        return sum(1 for _ in self.__iter__())
