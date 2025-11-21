import uuid
from unittest.mock import MagicMock

import pytest
import respx
from httpx import Response
from pydantic import HttpUrl, SecretStr

from tiled.access_control.scopes import NO_SCOPES
from tiled.queries import AccessBlobFilter

from ..access_control.access_policies import ExternalPolicyDecisionPoint
from ..server.schemas import Principal, PrincipalType


@pytest.fixture
def external_policy() -> ExternalPolicyDecisionPoint:
    return ExternalPolicyDecisionPoint(
        authorization_provider=HttpUrl("http://example.com"),
        audience="aud",
        node_access="allow",
        filter_nodes="tags",
        scopes_access="scopes",
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
    respx.post(external_policy._node_access).mock(
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
    respx.post(external_policy._node_access).mock(
        return_value=Response(200, json={"result": False})
    )
    with pytest.raises(ValueError, match="Permission denied not able to add the node"):
        await external_policy.init_node(
            principal=principal,
            authn_access_tags=set(),
            authn_scopes=set([]),
            access_blob={"tags": {"beamline_x_user"}},
        )


@pytest.mark.asyncio
@respx.mock
async def test_node_modify_allowed(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    node = MagicMock()
    node.access_blob = None
    respx.post(external_policy._node_access).mock(
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
    respx.post(external_policy._node_access).mock(
        return_value=Response(200, json={"result": False})
    )
    with pytest.raises(ValueError, match="Permission denied not able to add the node"):
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
    respx.post(external_policy._filter_nodes).mock(
        return_value=Response(200, json=output)
    )

    filters = await external_policy.filters(
        node=MagicMock(),
        principal=principal,
        authn_access_tags=set(),
        authn_scopes=set([]),
        scopes=set([]),
    )
    assert filters == [AccessBlobFilter(tags=output["result"], user_id=None)]


@pytest.mark.asyncio
@respx.mock
async def test_allowed_scopes(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    output = {"result": ["read:data", "write:data"]}
    respx.post(external_policy._scopes_access).mock(
        return_value=Response(200, json=output)
    )

    allowed_scopes = await external_policy.allowed_scopes(
        node=None,
        principal=principal,
        authn_access_tags=set(),
        authn_scopes=set([]),
    )
    assert allowed_scopes == set(output["result"])


@pytest.mark.asyncio
@respx.mock
async def test_allowed_scopes_return_no_scopes_if_invalid_response(
    external_policy: ExternalPolicyDecisionPoint, principal: Principal
):
    respx.post(external_policy._scopes_access).mock(
        return_value=Response(200, json={"result": True})
    )

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
    respx.post(external_policy._scopes_access).mock(
        return_value=Response(200, json=True)
    )

    allowed_scopes = await external_policy.allowed_scopes(
        node=None,
        principal=principal,
        authn_access_tags=set(),
        authn_scopes=set([]),
    )
    assert allowed_scopes == NO_SCOPES


def test_identifier_method_for_external_principal_with_no_access_token(
    external_policy: ExternalPolicyDecisionPoint,
):
    principal = Principal(
        type=PrincipalType.external,
        identities=[],
        uuid=uuid.uuid4(),
        access_token=None,
    )

    with pytest.raises(
        ValueError, match="Access token not provided for external principal type"
    ):
        external_policy._identifier(principal)


def test_identifier_method_for_external_principal(
    external_policy: ExternalPolicyDecisionPoint,
):
    principal = Principal(
        type=PrincipalType.external,
        identities=[],
        uuid=uuid.uuid4(),
        access_token=SecretStr("token123"),
    )

    assert external_policy._identifier(principal) == "token123"


def test_identifier_method_for_service_principal(
    external_policy: ExternalPolicyDecisionPoint,
):
    principal_uuid = uuid.uuid4()
    principal = Principal(
        type=PrincipalType.service,
        identities=[],
        uuid=principal_uuid,
        access_token=None,
    )

    assert external_policy._identifier(principal) == str(principal_uuid)
