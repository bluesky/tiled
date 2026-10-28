"""Tests for `tiled.storage`."""

import pyarrow as pa
import pytest

from tiled.adapters.sql import SQLAdapter
from tiled.storage import (
    _STORAGE_REFCOUNTS,
    AmbiguousStorage,
    RemoteSQLStorage,
    acquire_storage,
    get_storage,
    parse_storage,
    register_storage,
    release_storage,
    size_from_uri,
    stat_uri,
    storage_registry_key,
    strip_connection_parameters,
    unregister_storage,
)
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import DataSource, Management
from tiled.structures.table import TableStructure

FLAG = "TILED_STRIP_STORAGE_URI_QUERY_ON_WRITE"


@pytest.mark.parametrize(
    "uri, expected",
    [
        # Nothing to strip: returned unchanged.
        ("postgresql://db:5432/storage", "postgresql://db:5432/storage"),
        ("postgresql://a:5432,b:5432/storage", "postgresql://a:5432,b:5432/storage"),
        # Connection-tuning parameters are stripped.
        ("postgresql://db/storage?connect_timeout=5", "postgresql://db/storage"),
        (
            "postgresql://a:5432,b:5432,c:5432/storage?target_session_attrs=read-write",
            "postgresql://a:5432,b:5432,c:5432/storage",
        ),
        (
            "postgresql://db/storage?sslmode=require&application_name=tiled",
            "postgresql://db/storage",
        ),
        # A password in the query string is never written.
        ("postgresql://db/storage?user=u&password=p", "postgresql://db/storage"),
        # Location parameters are kept verbatim and in order.
        (
            "postgresql://localhost/storage?host=pg1&connect_timeout=5&host=pg2"
            "&target_session_attrs=read-write",
            "postgresql://localhost/storage?host=pg1&host=pg2",
        ),
        (
            "postgresql://localhost/storage?host=%2Fvar%2Frun%2Fpostgresql&port=5433",
            "postgresql://localhost/storage?host=%2Fvar%2Frun%2Fpostgresql&port=5433",
        ),
        ("postgres://db/storage?connect_timeout=5", "postgres://db/storage"),
        # Other schemes are untouched.
        ("sqlite:////tmp/x.db?mode=ro", "sqlite:////tmp/x.db?mode=ro"),
        ("duckdb:////tmp/x.duckdb?a=1", "duckdb:////tmp/x.duckdb?a=1"),
        ("file://localhost/data?x=1", "file://localhost/data?x=1"),
    ],
)
def test_strip_connection_parameters(uri, expected):
    assert strip_connection_parameters(uri) == expected
    # Idempotent
    assert strip_connection_parameters(expected) == expected


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("postgresql://db:5432/storage", "postgresql:///storage"),
        ("postgres://db/storage", "postgresql:///storage"),
        (
            "postgresql://u:p@a:1,b:2,c:3/storage?sslmode=require",
            "postgresql:///storage",
        ),
        ("postgresql://localhost/storage?host=pg1&host=pg2", "postgresql:///storage"),
        # dbname in the query takes precedence, as in libpq.
        ("postgresql://localhost/ignored?dbname=storage", "postgresql:///storage"),
        ("postgresql://db/my%20db", "postgresql:///my%20db"),
        # Other schemes are untouched.
        ("sqlite:////tmp/x.db", "sqlite:////tmp/x.db"),
        ("duckdb:////tmp/x.duckdb", "duckdb:////tmp/x.duckdb"),
    ],
)
def test_storage_registry_key(uri, expected):
    assert storage_registry_key(uri) == expected


CONFIGURED_URI = (
    "postgresql://user:secret@pg2:5432,pg1:5432,pg3:5432/storage"
    "?connect_timeout=5&target_session_attrs=read-write"
)


@pytest.fixture
def registered_storage():
    "A multi-host PostgreSQL storage configured with connection-tuning params."
    storage = RemoteSQLStorage(CONFIGURED_URI)
    register_storage(storage)
    yield storage
    unregister_storage(storage)


@pytest.mark.parametrize(
    "data_uri",
    [
        # Written by older versions, as configured now
        "postgresql://pg2:5432,pg1:5432,pg3:5432/storage"
        "?connect_timeout=5&target_session_attrs=read-write",
        # ...or with different connection-tuning parameters
        "postgresql://pg2:5432,pg1:5432,pg3:5432/storage?connect_timeout=30",
        # Written with the feature flag on
        "postgresql://pg2:5432,pg1:5432,pg3:5432/storage",
        # Different host order
        "postgresql://pg1:5432,pg2:5432,pg3:5432/storage",
        # Different cluster membership
        "postgresql://pg1:5432,pg2:5432/storage",
        # Via a proxy / pooler endpoint
        "postgresql://haproxy:5000/storage",
        # Hosts given in the query string
        "postgresql://localhost/storage?host=pg1&host=pg2",
    ],
)
def test_get_storage_matches_on_database_name(registered_storage, data_uri):
    "Any Asset.data_uri for this database resolves, however it was reached."
    assert get_storage(data_uri) is registered_storage


def test_get_storage_different_database_does_not_match(registered_storage):
    with pytest.raises(KeyError):
        get_storage("postgresql://pg1:5432,pg2:5432,pg3:5432/other_database")
    with pytest.raises(KeyError):
        get_storage("postgresql://pg1:5432/ignored?dbname=other_database")


def test_reregistering_equal_storage_is_allowed(registered_storage):
    "Each catalog mount may register its own equal copy."
    other = RemoteSQLStorage(CONFIGURED_URI)
    assert other is not registered_storage
    register_storage(other)
    assert get_storage("postgresql://anywhere/storage") is other


@pytest.mark.parametrize(
    "uri, kwargs, differing",
    [
        # Another server with a database of the same name
        ("postgresql://user:secret@elsewhere:5432/storage", {}, "uri"),
        # Same servers, listed differently
        (
            "postgresql://user:secret@pg1:5432,pg2:5432,pg3:5432/storage"
            "?connect_timeout=5&target_session_attrs=read-write",
            {},
            "uri",
        ),
        # Different connection-tuning parameters
        (
            "postgresql://user:secret@pg2:5432,pg1:5432,pg3:5432/storage"
            "?connect_timeout=9&target_session_attrs=read-write",
            {},
            "uri",
        ),
        # Different credentials: a shared pool would connect as one of them
        (
            "postgresql://other:pw@pg2:5432,pg1:5432,pg3:5432/storage"
            "?connect_timeout=5&target_session_attrs=read-write",
            {},
            "username, password",
        ),
        # Different pool settings
        (CONFIGURED_URI, {"pool_size": 7}, "pool_size"),
        (CONFIGURED_URI, {"max_overflow": 3}, "max_overflow"),
    ],
)
def test_conflicting_storage_is_refused(registered_storage, uri, kwargs, differing):
    with pytest.raises(AmbiguousStorage, match=f"differing: {differing}") as info:
        register_storage(RemoteSQLStorage(uri, **kwargs))
    # Passwords never appear in the message.
    assert "secret" not in str(info.value)
    assert "pw" not in str(info.value)
    # The original registration is intact.
    assert get_storage("postgresql://anywhere/storage") is registered_storage


def test_credentials_retained_for_connecting(registered_storage):
    "Matching must not alter the URI used to connect."
    assert registered_storage.authenticated_uri == CONFIGURED_URI


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, statement):
        pass

    def fetchone(self):
        return (1,)


class _FakeConnection:
    def cursor(self):
        return _FakeCursor()

    def commit(self):
        pass

    def close(self):
        pass


class _StubRemoteSQLStorage(RemoteSQLStorage):
    "Behaves like PostgreSQL storage without needing a server."

    def connect(self):
        return _FakeConnection()


def _init_sql_data_source(storage):
    table = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["x"])
    data_source = DataSource(
        management=Management.writable,
        mimetype="application/x-tiled-sql-table",
        structure_family=StructureFamily.table,
        structure=TableStructure.from_arrow_table(table, npartitions=1),
        parameters={},
        assets=[],
    )
    return SQLAdapter.init_storage(data_source=data_source, storage=storage)


@pytest.mark.parametrize(
    "flag, expected_data_uri",
    [
        # Default: legacy behavior, credentials stripped but query retained
        (
            None,
            "postgresql://db.example:5432/storage"
            "?connect_timeout=5&target_session_attrs=read-write",
        ),
        (
            "0",
            "postgresql://db.example:5432/storage"
            "?connect_timeout=5&target_session_attrs=read-write",
        ),
        # Flag on: query stripped too
        ("1", "postgresql://db.example:5432/storage"),
    ],
)
def test_strip_query_on_write_feature_flag(monkeypatch, flag, expected_data_uri):
    if flag is None:
        monkeypatch.delenv(FLAG, raising=False)
    else:
        monkeypatch.setenv(FLAG, flag)
    storage = _StubRemoteSQLStorage(
        "postgresql://user:secret@db.example:5432/storage"
        "?connect_timeout=5&target_session_attrs=read-write"
    )
    data_source = _init_sql_data_source(storage)
    (asset,) = data_source.assets
    assert asset.data_uri == expected_data_uri
    assert "secret" not in asset.data_uri


def test_parse_storage_shares_equal_sql_storage(tmp_path):
    uri = f"sqlite:///{tmp_path}/storage.db"
    a = parse_storage(uri)
    assert parse_storage(uri) is a
    # A different configuration is a different instance.
    b = parse_storage(uri, pool_size=7)
    assert b is not a
    assert parse_storage(uri, pool_size=7) is b
    # Credentials are part of the configuration.
    pg = "postgresql://u:p@db.example/storage"
    assert parse_storage(pg) is parse_storage(pg)
    assert parse_storage(pg) is not parse_storage("postgresql://v:p@db.example/storage")


def test_parse_storage_does_not_share_file_storage(tmp_path):
    "Only SQL storage holds connections; nothing to share otherwise."
    assert parse_storage(tmp_path) is not parse_storage(tmp_path)


@pytest.fixture
def two_catalogs_sharing_sql_storage(tmp_path):
    "Two catalog mounts configured with the same SQL storage, as in prod."
    from tiled.catalog import from_uri

    storage_uri = f"sqlite:///{tmp_path}/storage.db"
    catalogs = [
        from_uri(
            f"sqlite:///{tmp_path}/catalog.db",
            writable_storage=[storage_uri],
            storage_pool_size=3,
        )
        for _ in range(2)
    ]
    yield catalogs
    for storage in catalogs[0].context.writable_storage.values():
        storage.dispose()
        unregister_storage(storage)


def test_catalog_mounts_share_one_sql_storage_pool(two_catalogs_sharing_sql_storage):
    a, b = two_catalogs_sharing_sql_storage
    (storage_a,) = a.context.writable_storage.values()
    (storage_b,) = b.context.writable_storage.values()
    # One instance, which is also what Assets resolve to.
    assert storage_a is storage_b
    assert get_storage(storage_a.uri) is storage_a

    # Table creation goes through each mount's own Context, reads through the
    # registry; all of it should use one pool.
    _init_sql_data_source(storage_a)
    _init_sql_data_source(storage_b)
    with get_storage(storage_a.uri).connect():
        pass
    assert storage_a._connection_pool is storage_b._connection_pool
    assert storage_a._connection_pool.size() == 3


def test_catalog_mounts_share_postgresql_connections(tmp_path, postgres_uri):
    "With PostgreSQL storage, count the server-side connections directly."
    from tiled.catalog import from_uri

    catalogs = [
        from_uri(f"sqlite:///{tmp_path}/catalog.db", writable_storage=[postgres_uri])
        for _ in range(3)
    ]
    storages = [
        storage
        for catalog in catalogs
        for storage in catalog.context.writable_storage.values()
    ]
    try:
        for storage in storages:
            _init_sql_data_source(storage)
        with storages[0].connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database()"
                )
                (count,) = cursor.fetchone()
        # One template connection plus one pooled connection (reused for each
        # sequential checkout), rather than that pair per catalog mount.
        assert count == 2
    finally:
        storages[0].dispose()
        unregister_storage(storages[0])


def _is_open(storage):
    "Whether the storage currently holds a connection pool."
    return "_connection_pool" in storage.__dict__


def test_acquire_release_refcount(tmp_path):
    storage = parse_storage(f"sqlite:///{tmp_path}/storage.db")
    assert acquire_storage(storage) is storage
    assert acquire_storage(parse_storage(f"sqlite:///{tmp_path}/storage.db")) is storage
    _init_sql_data_source(storage)
    assert _is_open(storage)

    release_storage(storage)
    # Still referenced once: registered, shared, and open.
    assert get_storage(storage.uri) is storage
    assert parse_storage(f"sqlite:///{tmp_path}/storage.db") is storage
    assert _is_open(storage)

    release_storage(storage)
    # Last reference released: closed, unregistered, and no longer shared.
    assert not _is_open(storage)
    with pytest.raises(KeyError):
        get_storage(storage.uri)
    assert parse_storage(f"sqlite:///{tmp_path}/storage.db") is not storage


def test_release_without_acquire_is_harmless(tmp_path):
    storage = parse_storage(f"sqlite:///{tmp_path}/storage.db")
    release_storage(storage)
    assert not _is_open(storage)
    # No negative count is left behind to swallow a later acquire.
    assert storage_registry_key(storage.uri) not in _STORAGE_REFCOUNTS


def test_refcount_entries(tmp_path):
    "Counts track acquires and releases; no zero entries linger."
    storage = parse_storage(f"sqlite:///{tmp_path}/storage.db")
    key = storage_registry_key(storage.uri)
    assert key not in _STORAGE_REFCOUNTS
    acquire_storage(storage)
    acquire_storage(storage)
    assert _STORAGE_REFCOUNTS[key] == 2
    release_storage(storage)
    assert _STORAGE_REFCOUNTS[key] == 1
    release_storage(storage)
    assert key not in _STORAGE_REFCOUNTS
    # Counting restarts cleanly.
    storage = acquire_storage(storage)
    assert _STORAGE_REFCOUNTS[key] == 1
    release_storage(storage)
    assert key not in _STORAGE_REFCOUNTS


async def test_catalog_shutdown_releases_storage(tmp_path):
    "The storage pool closes when the last catalog mount using it shuts down."
    from tiled.catalog import from_uri

    def make_catalog():
        return from_uri(
            f"sqlite:///{tmp_path}/catalog.db",
            writable_storage=[f"sqlite:///{tmp_path}/storage.db"],
            init_if_not_exists=True,
        )

    a, b = make_catalog(), make_catalog()
    await a.startup()
    await b.startup()
    (storage,) = a.context.writable_storage.values()
    assert b.context.writable_storage[storage.uri] is storage
    _init_sql_data_source(storage)
    assert _is_open(storage)

    await a.shutdown()
    # b still uses it.
    assert _is_open(storage)
    assert get_storage(storage.uri) is storage
    await a.shutdown()  # repeated shutdown does not release again
    assert _is_open(storage)

    await b.shutdown()
    assert not _is_open(storage)
    with pytest.raises(KeyError):
        get_storage(storage.uri)

    # A mount created afterward gets a fresh, working instance...
    c = make_catalog()
    await c.startup()
    (new_storage,) = c.context.writable_storage.values()
    assert new_storage is not storage
    _init_sql_data_source(new_storage)
    # ...and restarting an earlier mount shares it.
    await a.startup()
    assert a.context.writable_storage[storage.uri] is new_storage
    assert get_storage(storage.uri) is new_storage

    await a.shutdown()
    assert _is_open(new_storage)
    await c.shutdown()
    assert not _is_open(new_storage)


def test_dispose_then_reuse(tmp_path):
    "A shared Storage may be disposed and then used again."
    storage = parse_storage(f"sqlite:///{tmp_path}/storage.db")
    storage.dispose()  # never connected: no-op
    _init_sql_data_source(storage)
    first_pool = storage._connection_pool
    storage.dispose()
    storage.dispose()  # idempotent
    _init_sql_data_source(storage)
    assert storage._connection_pool is not first_pool
    storage.dispose()


def test_size_from_uri_file(tmp_path):
    """`size_from_uri` returns the byte length of a local file:// URI."""
    p = tmp_path / "blob.bin"
    payload = b"the quick brown fox"
    p.write_bytes(payload)
    assert size_from_uri(p.as_uri()) == len(payload)


def test_size_from_uri_unsupported_scheme():
    """`size_from_uri` rejects schemes outside `file` and the supported
    object-store set with a clear `ValueError`."""
    with pytest.raises(ValueError, match="unsupported scheme"):
        size_from_uri("ftp://example.com/blob.bin")


def test_size_from_uri_missing_file(tmp_path):
    """Underlying I/O errors propagate; callers wanting best-effort behavior
    must catch them at the call site."""
    missing = tmp_path / "does-not-exist.bin"
    with pytest.raises(FileNotFoundError):
        size_from_uri(missing.as_uri())


def test_stat_uri_file(tmp_path):
    """`stat_uri` reports a regular file as not-a-directory with its size."""
    p = tmp_path / "blob.bin"
    payload = b"the quick brown fox"
    p.write_bytes(payload)
    result = stat_uri(p.as_uri())
    assert result == (False, len(payload))
    # Fields are also accessible by name.
    assert result.is_directory is False
    assert result.size == len(payload)


def test_stat_uri_directory(tmp_path):
    """`stat_uri` reports a directory as such, with no size."""
    d = tmp_path / "store.zarr"
    d.mkdir()
    assert stat_uri(d.as_uri()) == (True, None)


def test_stat_uri_missing_file_is_best_effort(tmp_path):
    """Unlike `size_from_uri`, `stat_uri` swallows I/O errors and returns
    `(False, None)` since the fields are advisory."""
    missing = tmp_path / "does-not-exist.bin"
    assert stat_uri(missing.as_uri()) == (False, None)


def test_stat_uri_unsupported_scheme_is_best_effort():
    """An un-inspectable scheme yields `(False, None)` rather than raising."""
    assert stat_uri("ftp://example.com/blob.bin") == (False, None)
