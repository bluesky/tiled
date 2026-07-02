import sqlite3
import uuid
from typing import Optional
from unittest.mock import MagicMock

import pytest
import respx
from httpx import Response
from pydantic import HttpUrl, SecretStr

from tiled.access_control.access_policies import ExternalPolicyDecisionPoint
from tiled.access_control.protocols import AccessPolicy
from tiled.access_control.scopes import NO_SCOPES, PUBLIC_SCOPES
from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.queries import AccessBlobFilter, AccessBlobInheritedFilter
from tiled.server.app import build_app
from tiled.server.schemas import Principal, PrincipalType
from tiled.type_aliases import AccessBlob, AccessTags, Scopes


@pytest.fixture
def external_policy() -> ExternalPolicyDecisionPoint:
    class TestExternalPolicyDecisionPoint(ExternalPolicyDecisionPoint):
        def build_input(
            self,
            principal: Principal,
            authn_access_tags: Optional[AccessTags],
            authn_scopes: Scopes,
            access_blob: Optional[AccessBlob] = None,
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
        authn_access_tags=set(),
        authn_scopes=set([]),
        access_blob={"tags": {"beamline_x_user"}},
    ) == (True, {"tags": {"beamline_x_user"}})


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
        authn_access_tags=set(),
        authn_scopes=set([]),
        access_blob={"tags": {"beamline_x_user"}},
    ) == (False, {"tags": {"beamline_x_user"}})


@pytest.mark.asyncio
@respx.mock
async def test_node_modify_allowed(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    node = MagicMock()
    node.access_blob = None
    respx.post(external_policy._create_node).mock(
        return_value=Response(200, json={"result": True})
    )
    assert await external_policy.modify_node(
        node=node,
        principal=principal,
        authn_access_tags=set(),
        authn_scopes=set([]),
        access_blob={"tags": {"beamline_x_user"}},
    ) == (True, {"tags": {"beamline_x_user"}})


@pytest.mark.asyncio
@respx.mock
async def test_node_modify_denied(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    node = MagicMock()
    node.access_blob = None
    respx.post(external_policy._create_node).mock(
        return_value=Response(200, json={"result": False})
    )
    assert await external_policy.modify_node(
        node=node,
        principal=principal,
        authn_access_tags=set(),
        authn_scopes=set([]),
        access_blob={"tags": {"beamline_x_user"}},
    ) == (False, {"tags": {"beamline_x_user"}})


@pytest.mark.asyncio
@respx.mock
async def test_node_modify_denied_when_none_returned(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    node = MagicMock()
    node.access_blob = None
    respx.post(external_policy._create_node).mock(return_value=Response(200))
    with pytest.raises(ValueError):
        await external_policy.modify_node(
            node=node,
            principal=principal,
            authn_access_tags=set(),
            authn_scopes=set([]),
            access_blob={"tags": {"beamline_x_user"}},
        )


@pytest.mark.asyncio
async def test_node_modify_with_same_not_modified(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    node = MagicMock()
    node.access_blob = {"tags": {"beamline_x_user"}}
    assert await external_policy.modify_node(
        node=node,
        principal=principal,
        authn_access_tags=set(),
        authn_scopes=set([]),
        access_blob={"tags": {"beamline_x_user"}},
    ) == (False, {"tags": {"beamline_x_user"}})


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
        authn_access_tags=set(),
        authn_scopes=set([]),
        scopes=set([]),
    )
    assert filters == [AccessBlobFilter(tags=["beamline_x"], user_id=None)]


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
        authn_access_tags=set(),
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
        authn_access_tags=set(),
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
        authn_access_tags=set(),
        authn_scopes=set([]),
    )
    assert allowed_scopes == NO_SCOPES


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "allow,remote_allow", [(True, None), (False, None), (None, True), (None, False)]
)
async def test_empty_access_blob_public(
    external_policy: ExternalPolicyDecisionPoint,
    principal: Principal,
    allow: Optional[bool],
    remote_allow: Optional[bool],
):
    external_policy._empty_access_blob_public = allow
    policy = external_policy
    if remote_allow is not None:
        route = respx.post(policy._create_node).mock(
            return_value=Response(200, json={"result": remote_allow})
        )
    else:
        route = None

    assert await policy.init_node(
        principal=principal,
        authn_access_tags=set(),
        authn_scopes=set([]),
        access_blob=None,
    ) == (allow if allow is not None else remote_allow, None)

    if route:
        assert route.call_count == 1


# ---------------------------------------------------------------------------
# AccessBlobInheritedFilter tests (exercised via an inline access policy)
# ---------------------------------------------------------------------------

_INHERITED_CATALOG = "catalog_inherited_filter"


class _InheritedTagPolicy(AccessPolicy):
    """Minimal test-only policy that gates listings with AccessBlobInheritedFilter."""

    def __init__(self, user_id, tags):
        self._user_id = user_id
        self._tags = list(tags)

    async def init_node(
        self, principal, authn_access_tags, authn_scopes, access_blob=None
    ):
        return (False, access_blob)

    async def allowed_scopes(self, node, principal, authn_access_tags, authn_scopes):
        return PUBLIC_SCOPES

    async def filters(self, node, principal, authn_access_tags, authn_scopes, scopes):
        if not hasattr(node, "access_blob"):
            return []
        return [AccessBlobInheritedFilter(user_id=self._user_id, tags=self._tags)]


@pytest.fixture(scope="module")
def inherited_policy_clients(tmpdir_module):
    """
    Module-scoped clients for inherited filter policy tests.

    Tree structure:

    container_x  {"tags": ["project-x"]}
      leaf_x1    {}  <- inherits project-x
      leaf_x2    {}  <- inherits project-x
      sub_y      {"tags": ["project-y"]}
        leaf_y1  {}  <- inherits project-y (nearest ancestor wins)

    container_y  {"tags": ["project-y"]}
      leaf_y2    {}  <- inherits project-y

    owned_by_bill  {"user": "bill"}
    no_tags        {}

    Clients:
        alice:         tags=["project-x"],        user_id=None
        bob:           tags=["project-y"],        user_id=None
        bill:          tags=[],                   user_id="bill"
        multi:         tags=["project-x","project-y"], user_id=None
        bill_and_x:    tags=["project-x"],        user_id="bill"
        nobody:        tags=[],                   user_id=None
    """
    contexts = []

    # Populate the catalog as an unrestricted single-user app.
    setup_adapter = in_memory(
        writable_storage=str(tmpdir_module / "inherited"),
        named_memory=_INHERITED_CATALOG,
    )
    setup_ctx = Context.from_app(build_app(setup_adapter))
    contexts.append(setup_ctx)
    setup_client = from_context(setup_ctx)

    setup_client.create_container(key="container_x")
    setup_client["container_x"].create_container(key="leaf_x1")
    setup_client["container_x"].create_container(key="leaf_x2")
    setup_client["container_x"].create_container(key="sub_y")
    setup_client["container_x"]["sub_y"].create_container(key="leaf_y1")
    setup_client.create_container(key="container_y")
    setup_client["container_y"].create_container(key="leaf_y2")
    setup_client.create_container(key="owned_by_bill")
    setup_client.create_container(key="no_tags")

    db = sqlite3.connect(
        f"file:{_INHERITED_CATALOG}?mode=memory&cache=shared", uri=True
    )
    cursor = db.cursor()
    cursor.executemany(
        "UPDATE nodes SET access_blob = ? WHERE key = ?",
        [
            ('{"tags": ["project-x"]}', "container_x"),
            ('{"tags": ["project-y"]}', "sub_y"),
            ('{"tags": ["project-y"]}', "container_y"),
            ('{"user": "bill"}', "owned_by_bill"),
        ],
    )
    db.commit()
    db.close()

    user_specs = [
        ("alice", None, ["project-x"]),
        ("bob", None, ["project-y"]),
        ("bill", "bill", []),
        ("multi", None, ["project-x", "project-y"]),
        ("bill_and_x", "bill", ["project-x"]),
        ("nobody", None, []),
    ]
    clients = {}
    for name, user_id, tags in user_specs:
        adapter = in_memory(
            writable_storage=str(tmpdir_module / f"inherited_{name}"),
            named_memory=_INHERITED_CATALOG,
        )
        policy = _InheritedTagPolicy(user_id=user_id, tags=tags)
        ctx = Context.from_app(build_app(adapter, access_policy=policy))
        contexts.append(ctx)
        clients[name] = from_context(ctx)

    yield clients

    for ctx in reversed(contexts):
        ctx.close()


def test_inherited_policy_direct_tag_match(inherited_policy_clients):
    """A tagged node is visible to a user holding that tag."""
    assert "container_x" in inherited_policy_clients["alice"]
    assert "container_y" not in inherited_policy_clients["alice"]


def test_inherited_policy_child_inherits_parent_tag(inherited_policy_clients):
    """Untagged children of a tagged node inherit that tag."""
    cx = inherited_policy_clients["alice"]["container_x"]
    assert "leaf_x1" in cx
    assert "leaf_x2" in cx


def test_inherited_policy_tagged_child_shadows_parent(inherited_policy_clients):
    """sub_y's own tag shadows container_x's tag; leaf_y1 inherits from sub_y."""
    # alice (project-x) cannot see sub_y (has project-y) when listing container_x
    cx_alice = inherited_policy_clients["alice"]["container_x"]
    assert "sub_y" not in cx_alice

    # multi (project-x + project-y) can navigate all the way down
    # leaf_y1 inherits project-y from sub_y (nearest tagged ancestor), not project-x
    cx_multi = inherited_policy_clients["multi"]["container_x"]
    assert "sub_y" in cx_multi
    assert "leaf_y1" in cx_multi["sub_y"]


def test_inherited_policy_no_matching_tag(inherited_policy_clients):
    """Nodes tagged with a mismatched tag are not visible."""
    # container_y (project-y) is invisible to alice (project-x only)
    assert "container_y" not in inherited_policy_clients["alice"]
    # container_x (project-x) is invisible to bob (project-y only)
    assert "container_x" not in inherited_policy_clients["bob"]


def test_inherited_policy_user_id_direct_match(inherited_policy_clients):
    """A node with {"user": "bill"} is visible to user_id="bill" regardless of tags."""
    assert "owned_by_bill" in inherited_policy_clients["bill"]


def test_inherited_policy_user_id_wrong_user(inherited_policy_clients):
    """owned_by_bill is invisible to a user with a different user_id."""
    assert "owned_by_bill" not in inherited_policy_clients["alice"]


def test_inherited_policy_no_tags_no_user_returns_empty(inherited_policy_clients):
    """A user with no tags and no user_id sees nothing."""
    assert list(inherited_policy_clients["nobody"]) == []


def test_inherited_policy_untagged_node_not_matched(inherited_policy_clients):
    """A node with no tags anywhere in its ancestry is not visible."""
    assert "no_tags" not in inherited_policy_clients["alice"]
    assert "no_tags" not in inherited_policy_clients["bob"]


def test_inherited_policy_multiple_tags_match_any(inherited_policy_clients):
    """A user holding multiple tags sees nodes matched by any of them."""
    multi = inherited_policy_clients["multi"]
    assert "container_x" in multi
    assert "container_y" in multi


def test_inherited_policy_user_id_and_tags_combined(inherited_policy_clients):
    """user_id and tags are OR-combined: either match makes a node visible."""
    c = inherited_policy_clients["bill_and_x"]
    assert "owned_by_bill" in c  # matched by user_id
    assert "container_x" in c  # matched by project-x tag
