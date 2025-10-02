import math
import uuid

import numpy
import pytest
import respx
from httpx import Response
from pydantic import HttpUrl

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter

from ..access_control.access_policies import ExternalPolicyDecisionPoint
from ..server.schemas import Principal, PrincipalType


@pytest.fixture
def tree() -> MapAdapter:
    return MapAdapter(
        {
            "example": ArrayAdapter.from_array(
                numpy.array([0, 1, numpy.nan, -numpy.inf, numpy.inf]),
                metadata={
                    "infinity": math.inf,
                    "-infinity": -math.inf,
                    "nan": numpy.nan,
                },
            )
        },
        metadata={
            "infinity": math.inf,
            "-infinity": -math.inf,
            "nan": numpy.nan,
            "permission": "admin",
        },
    )


@pytest.mark.asyncio
@respx.mock
@pytest.mark.parametrize("result", [True, False])
async def test_authorized(result: bool, tree: MapAdapter):
    principal = Principal(
        type=PrincipalType.jwt_token,
        identities=[],
        uuid=uuid.uuid4(),
        access_token="token123",
    )

    actions = ["read:data"]

    respx.post("http://example.com").mock(
        return_value=Response(200, json={"result": result})
    )

    pdp = ExternalPolicyDecisionPoint(
        authorization_provider=HttpUrl("http://example.com"),
        audience="aud",
        attribute="foo",
    )

    result = await pdp.authorized(tree, principal, actions)
    assert result is result


@pytest.mark.asyncio
async def test_raise_error_if_access_token_not_provided(tree: MapAdapter):
    principal = Principal(
        type=PrincipalType.user,
        identities=[],
        uuid=uuid.uuid4(),
    )
    actions = ["read:data"]

    pdp = ExternalPolicyDecisionPoint(
        authorization_provider=HttpUrl("http://example.com"),
        audience="aud",
        attribute="foo",
    )
    with pytest.raises(
        RuntimeError, match="External policy access control requires a bearer token."
    ):
        await pdp.authorized(tree, principal, actions)


@pytest.mark.asyncio
@respx.mock
async def test_authorized_validation_failure(tree: MapAdapter):
    principal = Principal(
        type=PrincipalType.jwt_token,
        identities=[],
        uuid=uuid.uuid4(),
        access_token="token123",
    )

    actions = ["read:data"]

    respx.post("http://example.com").mock(
        return_value=Response(200, json={"foo": "bar"})
    )

    pdp = ExternalPolicyDecisionPoint(
        authorization_provider=HttpUrl("http://example.com"),
        audience="aud",
        attribute="foo",
    )

    result = await pdp.authorized(tree, principal, actions)
    assert result is False
