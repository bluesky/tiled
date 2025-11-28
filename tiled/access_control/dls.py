import json
import logging
from typing import Optional, TypedDict

from pydantic import HttpUrl, TypeAdapter

from tiled.access_control.access_policies import ExternalPolicyDecisionPoint

from ..server.schemas import Principal, PrincipalType
from ..type_aliases import AccessBlob, AccessTags, Scopes

logger = logging.getLogger(__name__)


class DiamondAccessBlob(TypedDict):
    proposal: int
    visit: int
    beamline: str


class DiamondOpenPolicyAgentAuthorizationPolicy(ExternalPolicyDecisionPoint):
    def __init__(
        self,
        authorization_provider: HttpUrl,
        token_audience: str,
        provider: Optional[str] = None,
        empty_access_blob_public: bool = False,
    ):
        self._token_audience = token_audience
        self._type_adapter = TypeAdapter(DiamondAccessBlob)

        super().__init__(
            authorization_provider,
            "session/write_to_beamline_visit",
            "session/user_sessions",
            "tiled/scopes",
            provider,
            empty_access_blob_public,
        )

    def build_input(
        self,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_blob: Optional[AccessBlob] = None,
    ) -> str:
        if self._token_audience is None:
            raise ValueError("Provider not set, cannot validate token audience")
        if (
            principal.type is not PrincipalType.external
            or principal.access_token is None
        ):
            raise ValueError("Access token not provided for external principal type")
        blob = (
            self._type_adapter.validate_json(access_blob["tags"][0])
            if access_blob
            else {}
        )

        return json.dumps(
            {
                "input": {
                    **blob,
                    "token": principal.access_token.get_secret_value(),
                    "audience": self._token_audience,
                }
            }
        )
