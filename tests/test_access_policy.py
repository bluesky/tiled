import uuid
from types import SimpleNamespace
from typing import Optional
from unittest.mock import MagicMock

import pytest
import respx
from httpx import Response
from pydantic import HttpUrl, SecretStr

from tiled.access_control.access_policies import (
    ExternalPolicyDecisionPoint,
    TagBasedAccessPolicy,
)
from tiled.access_control.protocols import (
    compiled_tags_version,
    mark_compiled_tags_changed,
    normalize_access_tags,
)
from tiled.access_control.scopes import NO_SCOPES
from tiled.queries import AccessTagsFilter
from tiled.server.schemas import Principal, PrincipalType
from tiled.type_aliases import AccessTags, Scopes


@pytest.fixture
def external_policy() -> ExternalPolicyDecisionPoint:
    class TestExternalPolicyDecisionPoint(ExternalPolicyDecisionPoint):
        def build_input(
            self,
            principal: Principal,
            authn_access_tags: Optional[AccessTags],
            authn_scopes: Scopes,
            access_tags: Optional[AccessTags] = None,
        ) -> str:
            return ""

    return TestExternalPolicyDecisionPoint(
        authorization_provider=HttpUrl("http://example.com"),
        create_node_endpoint="allow",
        allowed_tags_endpoint="tags",
        scopes_endpoint="scopes",
    )


@pytest.fixture
def principal() -> Principal:
    return Principal(
        type=PrincipalType.user,
        identities=[{"id": "alice", "provider": "dummy"}],
        uuid=uuid.uuid4(),
        access_token=SecretStr("token123"),
    )


@pytest.mark.asyncio
@respx.mock
async def test_node_access_allowed(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    respx.post(external_policy._create_node).mock(
        return_value=Response(200, json={"result": True})
    )
    assert await external_policy.init_node(
        principal=principal,
        authn_access_tags=normalize_access_tags(),
        authn_scopes=set([]),
        access_tags=normalize_access_tags(["beamline_x_user"]),
    ) == (True, normalize_access_tags(["beamline_x_user"]))


@pytest.mark.asyncio
@respx.mock
async def test_node_access_denied(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    respx.post(external_policy._create_node).mock(
        return_value=Response(200, json={"result": False})
    )
    assert await external_policy.init_node(
        principal=principal,
        authn_access_tags=normalize_access_tags(),
        authn_scopes=set([]),
        access_tags=normalize_access_tags(["beamline_x_user"]),
    ) == (False, normalize_access_tags(["beamline_x_user"]))


@pytest.mark.asyncio
@respx.mock
async def test_node_modify_allowed(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    node = MagicMock()
    node.access_tags = None
    respx.post(external_policy._create_node).mock(
        return_value=Response(200, json={"result": True})
    )
    assert await external_policy.modify_node(
        node=node,
        principal=principal,
        authn_access_tags=normalize_access_tags(),
        authn_scopes=set([]),
        access_tags=normalize_access_tags(["beamline_x_user"]),
    ) == (True, normalize_access_tags(["beamline_x_user"]))


@pytest.mark.asyncio
@respx.mock
async def test_node_modify_denied(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    node = MagicMock()
    node.access_tags = None
    respx.post(external_policy._create_node).mock(
        return_value=Response(200, json={"result": False})
    )
    assert await external_policy.modify_node(
        node=node,
        principal=principal,
        authn_access_tags=normalize_access_tags(),
        authn_scopes=set([]),
        access_tags=normalize_access_tags(["beamline_x_user"]),
    ) == (False, normalize_access_tags(["beamline_x_user"]))


@pytest.mark.asyncio
@respx.mock
async def test_node_modify_denied_when_none_returned(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    node = MagicMock()
    node.access_tags = None
    respx.post(external_policy._create_node).mock(return_value=Response(200))
    with pytest.raises(ValueError):
        await external_policy.modify_node(
            node=node,
            principal=principal,
            authn_access_tags=normalize_access_tags(),
            authn_scopes=set([]),
            access_tags=normalize_access_tags(["beamline_x_user"]),
        )


@pytest.mark.asyncio
async def test_node_modify_with_same_not_modified(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    node = MagicMock()
    node.access_tags = normalize_access_tags(["beamline_x_user"])
    assert await external_policy.modify_node(
        node=node,
        principal=principal,
        authn_access_tags=normalize_access_tags(),
        authn_scopes=set([]),
        access_tags=normalize_access_tags(["beamline_x_user"]),
    ) == (False, normalize_access_tags(["beamline_x_user"]))


@pytest.mark.asyncio
@respx.mock
async def test_access_filters(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    output = {"result": ["beamline_x"]}
    respx.post(external_policy._user_tags).mock(return_value=Response(200, json=output))

    filters = await external_policy.filters(
        node=MagicMock(),
        principal=principal,
        authn_access_tags=normalize_access_tags(),
        authn_scopes=set([]),
        scopes=set([]),
    )
    assert filters == [AccessTagsFilter(tags=["beamline_x"])]


@pytest.mark.asyncio
@respx.mock
async def test_allowed_scopes(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    respx.post(external_policy._node_scopes).mock(
        return_value=Response(200, json={"result": ["read:data", "write:data"]})
    )

    allowed_scopes = await external_policy.allowed_scopes(
        node=None,
        principal=principal,
        authn_access_tags=normalize_access_tags(),
        authn_scopes=set([]),
    )
    assert allowed_scopes == {"read:data", "write:data"}


@pytest.mark.asyncio
@respx.mock
async def test_allowed_scopes_return_no_scopes_if_invalid_response(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    respx.post(external_policy._node_scopes).mock(return_value=Response(200))

    allowed_scopes = await external_policy.allowed_scopes(
        node=None,
        principal=principal,
        authn_access_tags=normalize_access_tags(),
        authn_scopes=set([]),
    )
    assert allowed_scopes == NO_SCOPES


@pytest.mark.asyncio
@respx.mock
async def test_allowed_scopes_return_no_scopes_if_validation_error(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    respx.post(external_policy._node_scopes).mock(return_value=Response(200, json=True))

    allowed_scopes = await external_policy.allowed_scopes(
        node=None,
        principal=principal,
        authn_access_tags=normalize_access_tags(),
        authn_scopes=set([]),
    )
    assert allowed_scopes == NO_SCOPES


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "allow,remote_allow", [(True, None), (False, None), (None, True), (None, False)]
)
async def test_empty_access_tags_public(
    external_policy: ExternalPolicyDecisionPoint,
    principal: Principal,
    allow: Optional[bool],
    remote_allow: Optional[bool],
):
    external_policy._empty_access_tags_public = allow
    policy = external_policy
    if remote_allow is not None:
        route = respx.post(policy._create_node).mock(
            return_value=Response(200, json={"result": remote_allow})
        )
    else:
        route = None

    assert await policy.init_node(
        principal=principal,
        authn_access_tags=normalize_access_tags(),
        authn_scopes=set([]),
        access_tags=None,
    ) == (allow if allow is not None else remote_allow, normalize_access_tags())

    if route:
        assert route.call_count == 1


# ---- Granted-tags cache (TagBasedAccessPolicy.filters) ----
# The cache holds the set of tags granted to a principal for the requested
# scopes, derived from the compiled grant tables; entries are validated
# against compiled_tags_version(). See also the catalog-side tests for the
# resolved tag-id cache in test_catalog.py.

GRANTED_TAGS_PARSER = "tiled.access_control.access_tags:AccessTagsParser"


def make_cached_policy(granted_tags, ttl=60):
    "A TagBasedAccessPolicy whose database lookups are counted fakes."
    policy = TagBasedAccessPolicy(
        provider="toy",
        access_tags_parser=GRANTED_TAGS_PARSER,
        granted_tags_cache_ttl=ttl,
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


def make_cached_policy_principal(name="alice"):
    identity = SimpleNamespace(provider="toy", id=name)
    return SimpleNamespace(type="user", uuid=uuid.uuid4(), identities=[identity])


# A node-like object: the policy only checks hasattr(node, "access_tags").
TAGGED_NODE = SimpleNamespace(access_tags=frozenset())
REQUESTED_SCOPES = {"read:metadata"}
AUTHN_SCOPES = {"read:metadata", "read:data"}


@pytest.mark.asyncio
async def test_granted_tags_cache_reuses_computed_tag_set():
    policy, calls = make_cached_policy({"beamline_x"})
    principal = make_cached_policy_principal()
    first = await policy.filters(
        TAGGED_NODE, principal, None, AUTHN_SCOPES, set(REQUESTED_SCOPES)
    )
    second = await policy.filters(
        TAGGED_NODE, principal, None, AUTHN_SCOPES, set(REQUESTED_SCOPES)
    )
    assert calls["grants"] == 1  # second call was a cache hit
    assert first[0].tags == second[0].tags
    # A fresh filter instance per call: mutating one (as the server does when
    # resolving tag_ids per catalog database) must not leak into the other.
    assert first[0] is not second[0]
    first[0].tag_ids = (1, 2, 3)
    assert second[0].tag_ids is None


@pytest.mark.asyncio
async def test_granted_tags_cache_disabled_with_zero_ttl():
    policy, calls = make_cached_policy({"beamline_x"}, ttl=0)
    principal = make_cached_policy_principal()
    await policy.filters(
        TAGGED_NODE, principal, None, AUTHN_SCOPES, set(REQUESTED_SCOPES)
    )
    await policy.filters(
        TAGGED_NODE, principal, None, AUTHN_SCOPES, set(REQUESTED_SCOPES)
    )
    assert calls["grants"] == 2


@pytest.mark.asyncio
async def test_granted_tags_cache_expires(monkeypatch):
    policy, calls = make_cached_policy({"beamline_x"}, ttl=60)
    principal = make_cached_policy_principal()
    fake_now = [1000.0]
    monkeypatch.setattr(
        "tiled.access_control.access_policies.time.monotonic", lambda: fake_now[0]
    )
    await policy.filters(
        TAGGED_NODE, principal, None, AUTHN_SCOPES, set(REQUESTED_SCOPES)
    )
    fake_now[0] += 59
    await policy.filters(
        TAGGED_NODE, principal, None, AUTHN_SCOPES, set(REQUESTED_SCOPES)
    )
    assert calls["grants"] == 1  # still fresh
    fake_now[0] += 2  # past the 60 s window; a hit does NOT extend it
    await policy.filters(
        TAGGED_NODE, principal, None, AUTHN_SCOPES, set(REQUESTED_SCOPES)
    )
    assert calls["grants"] == 2


@pytest.mark.asyncio
async def test_granted_tags_cache_invalidated_by_version_change():
    policy, calls = make_cached_policy({"beamline_x"})
    principal = make_cached_policy_principal()
    await policy.filters(
        TAGGED_NODE, principal, None, AUTHN_SCOPES, set(REQUESTED_SCOPES)
    )
    mark_compiled_tags_changed()
    await policy.filters(
        TAGGED_NODE, principal, None, AUTHN_SCOPES, set(REQUESTED_SCOPES)
    )
    assert calls["grants"] == 2


@pytest.mark.asyncio
async def test_granted_tags_cache_eviction_bounded(monkeypatch):
    monkeypatch.setattr(TagBasedAccessPolicy, "GRANTED_TAGS_CACHE_MAX_SIZE", 4)
    policy, _ = make_cached_policy({"beamline_x"})
    for i in range(10):
        await policy.filters(
            TAGGED_NODE,
            make_cached_policy_principal(f"user{i}"),
            None,
            AUTHN_SCOPES,
            set(REQUESTED_SCOPES),
        )
    assert len(policy._granted_tags_cache) <= 4


def test_compiled_tags_version_monotonic():
    v0 = compiled_tags_version()
    mark_compiled_tags_changed()
    assert compiled_tags_version() == v0 + 1
