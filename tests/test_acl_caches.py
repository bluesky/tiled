"""Tests for the ACL caches: the policy's per-principal tag-set cache and the
catalog context's resolved tag-id cache, and their invalidation by the
compiled-tags version."""

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select
from sqlalchemy.dialects import postgresql, sqlite

from tiled.access_control import protocols
from tiled.access_control.access_policies import TagBasedAccessPolicy
from tiled.access_control.protocols import (
    compiled_tags_version,
    mark_compiled_tags_changed,
)
from tiled.catalog import in_memory
from tiled.catalog import orm as catalog_orm
from tiled.catalog.adapter import _access_tags_predicate, _resolve_access_tags
from tiled.catalog.core import register_principal_tag_rows
from tiled.queries import AccessTagsFilter

PARSER = "tiled.access_control.access_tags:AccessTagsParser"


def make_policy(granted_tags, ttl=60):
    "A TagBasedAccessPolicy whose database lookups are counted fakes."
    policy = TagBasedAccessPolicy(
        provider="toy", access_tags_parser=PARSER, granted_tags_cache_ttl=ttl
    )
    calls = {"grants": 0, "public": 0}

    async def fake_tags_from_scope(scope, identifier):
        calls["grants"] += 1
        return set(granted_tags)

    async def fake_public_tags():
        calls["public"] += 1
        return {"public"}

    policy.get_tags_from_scope = fake_tags_from_scope
    policy.get_public_tags = fake_public_tags
    return policy, calls


def make_principal(name="alice"):
    identity = SimpleNamespace(provider="toy", id=name)
    return SimpleNamespace(type="user", uuid=uuid.uuid4(), identities=[identity])


# A node-like object: the policy only checks hasattr(node, "access_tags").
NODE = SimpleNamespace(access_tags=frozenset())
SCOPES = {"read:metadata"}
AUTHN_SCOPES = {"read:metadata", "read:data"}


@pytest.mark.asyncio
async def test_granted_tags_cache_reuses_computed_tag_set():
    policy, calls = make_policy({"beamline_x"})
    principal = make_principal()
    first = await policy.filters(NODE, principal, None, AUTHN_SCOPES, set(SCOPES))
    second = await policy.filters(NODE, principal, None, AUTHN_SCOPES, set(SCOPES))
    assert calls["grants"] == 1  # second call was a cache hit
    assert first[0].tags == second[0].tags
    # A fresh filter instance per call: mutating one (as the server does when
    # resolving tag_ids per catalog database) must not leak into the other.
    assert first[0] is not second[0]
    first[0].tag_ids = (1, 2, 3)
    assert second[0].tag_ids is None


@pytest.mark.asyncio
async def test_granted_tags_cache_disabled_with_zero_ttl():
    policy, calls = make_policy({"beamline_x"}, ttl=0)
    principal = make_principal()
    await policy.filters(NODE, principal, None, AUTHN_SCOPES, set(SCOPES))
    await policy.filters(NODE, principal, None, AUTHN_SCOPES, set(SCOPES))
    assert calls["grants"] == 2


@pytest.mark.asyncio
async def test_granted_tags_cache_expires(monkeypatch):
    policy, calls = make_policy({"beamline_x"}, ttl=60)
    principal = make_principal()
    fake_now = [1000.0]
    monkeypatch.setattr(
        "tiled.access_control.access_policies.time.monotonic", lambda: fake_now[0]
    )
    await policy.filters(NODE, principal, None, AUTHN_SCOPES, set(SCOPES))
    fake_now[0] += 59
    await policy.filters(NODE, principal, None, AUTHN_SCOPES, set(SCOPES))
    assert calls["grants"] == 1  # still fresh
    fake_now[0] += 2  # past the 60 s window; a hit does NOT extend it
    await policy.filters(NODE, principal, None, AUTHN_SCOPES, set(SCOPES))
    assert calls["grants"] == 2


@pytest.mark.asyncio
async def test_granted_tags_cache_invalidated_by_version_change():
    policy, calls = make_policy({"beamline_x"})
    principal = make_principal()
    await policy.filters(NODE, principal, None, AUTHN_SCOPES, set(SCOPES))
    mark_compiled_tags_changed()
    await policy.filters(NODE, principal, None, AUTHN_SCOPES, set(SCOPES))
    assert calls["grants"] == 2


@pytest.mark.asyncio
async def test_granted_tags_cache_eviction_bounded(monkeypatch):
    monkeypatch.setattr(TagBasedAccessPolicy, "GRANTED_TAGS_CACHE_MAX_SIZE", 4)
    policy, _ = make_policy({"beamline_x"})
    for i in range(10):
        await policy.filters(
            NODE, make_principal(f"user{i}"), None, AUTHN_SCOPES, set(SCOPES)
        )
    assert len(policy._granted_tags_cache) <= 4


def test_predicate_uses_array_parameter_on_postgresql():
    query = AccessTagsFilter(["a"])
    query.tag_ids = (30, 10, 20, 10)
    condition = _access_tags_predicate(query, catalog_orm.Node, 7, "postgresql")
    sql = str(
        condition.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert "= ANY (ARRAY[10, 20, 30])" in sql  # deduplicated, sorted, one array
    assert "IN (" not in sql
    sql_lite = str(
        condition.compile(
            dialect=postgresql.dialect()  # parameterized form: a single parameter
        )
    )
    assert sql_lite.count("%(") == 2  # parent id + the one array parameter


def test_predicate_uses_in_list_on_sqlite():
    query = AccessTagsFilter(["a"])
    query.tag_ids = (2, 1)
    condition = _access_tags_predicate(query, catalog_orm.Node, 7, "sqlite")
    sql = str(
        condition.compile(
            dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert "IN (1, 2)" in sql


@pytest.mark.asyncio
async def test_resolved_tag_ids_cached_per_catalog_context(tmpdir):
    "Two catalogs with different id spaces must not share resolved ids."
    # Distinct named in-memory databases: with the plain ':memory:' URI both
    # adapters would share one pooled engine (and thus one database).
    adapter_a = in_memory(named_memory="acl_cache_a", writable_storage=str(tmpdir / "a"))
    adapter_b = in_memory(named_memory="acl_cache_b", writable_storage=str(tmpdir / "b"))
    try:
        for adapter, names in [
            (adapter_a, ["tag_one", "tag_two"]),
            (adapter_b, ["tag_two", "tag_one"]),  # reversed insert order: ids differ
        ]:
            await adapter.startup()
            async with adapter.context.session() as db:
                for name in names:
                    await db.execute(
                        insert(catalog_orm.AccessTag.__table__).values(
                            name=name, is_public=False
                        )
                    )
                await db.commit()
        ids_a = await adapter_a.resolve_access_tag_ids(["tag_one"])
        ids_b = await adapter_b.resolve_access_tag_ids(["tag_one"])
        assert ids_a != ids_b  # different id spaces, correctly isolated
        # Second resolution is a cache hit: the same (immutable) tuple.
        assert await adapter_a.resolve_access_tag_ids(["tag_one"]) is ids_a
        assert isinstance(ids_a, tuple)
    finally:
        await adapter_a.shutdown()
        await adapter_b.shutdown()


@pytest.mark.asyncio
async def test_register_principal_tag_rows_reports_inserts(tmpdir):
    adapter = in_memory(writable_storage=str(tmpdir))
    try:
        await adapter.startup()
        async with adapter.context.session() as db:
            connection = await db.connection()
            assert await register_principal_tag_rows(connection, ["user:alice"])
            await db.commit()
        async with adapter.context.session() as db:
            connection = await db.connection()
            # Already registered: no insert, so caches need no invalidation.
            assert not await register_principal_tag_rows(connection, ["user:alice"])
            # Non-principal names never register rows.
            assert not await register_principal_tag_rows(connection, ["beamline_x"])
            await db.commit()
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_version_bumped_only_after_commit(tmpdir):
    "First-time principal-tag registration invalidates caches at COMMIT, not before."
    adapter = in_memory(writable_storage=str(tmpdir))
    try:
        await adapter.startup()
        before = compiled_tags_version()
        async with adapter.context.session() as db:
            await _resolve_access_tags(db, ["user:bob"])
            # The row is not yet visible to other transactions; the version
            # must not have moved yet.
            assert compiled_tags_version() == before
            await db.commit()
        assert compiled_tags_version() == before + 1
        # Re-registering the same principal tag changes nothing: no bump.
        async with adapter.context.session() as db:
            await _resolve_access_tags(db, ["user:bob"])
            await db.commit()
        assert compiled_tags_version() == before + 1
        # The row really is there.
        async with adapter.context.session() as db:
            row = (
                await db.execute(
                    select(catalog_orm.AccessTag).where(
                        catalog_orm.AccessTag.name == "user:bob"
                    )
                )
            ).scalar_one()
            assert row.name == "user:bob"
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_version_not_bumped_on_rollback(tmpdir):
    adapter = in_memory(writable_storage=str(tmpdir))
    try:
        await adapter.startup()
        before = compiled_tags_version()
        async with adapter.context.session() as db:
            await _resolve_access_tags(db, ["user:carol"])
            await db.rollback()
        assert compiled_tags_version() == before
    finally:
        await adapter.shutdown()


def test_version_counter_monotonic():
    v0 = protocols.compiled_tags_version()
    mark_compiled_tags_changed()
    assert protocols.compiled_tags_version() == v0 + 1
