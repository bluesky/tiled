import uuid
from typing import Optional
from unittest.mock import MagicMock

import pytest
import respx
from httpx import Response
from pydantic import HttpUrl, SecretStr

from tiled.access_control.scopes import NO_SCOPES
from tiled.queries import AccessBlobFilter
from tiled.type_aliases import AccessBlob, AccessTags, Scopes

from ..access_control.access_policies import ExternalPolicyDecisionPoint
from ..server.schemas import Principal, PrincipalType


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
        type=PrincipalType.external,
        identities=[],
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
