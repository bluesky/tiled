import json
from typing import Optional

from pydantic import HttpUrl

from tiled.access_control.access_policies import ExternalPolicyDecisionPoint
from tiled.server.schemas import Principal, PrincipalType
from tiled.type_aliases import AccessBlob, AccessTags, Scopes


class ExampleAuthorizationPolicy(ExternalPolicyDecisionPoint):
    def __init__(
        self,
        authorization_provider: HttpUrl,
        token_audience: str,
        create_node_endpoint: str,
        allowed_tags_endpoint: str,
        scopes_endpoint: str,
        modify_node_endpoint: Optional[str] = None,
        empty_access_blob_public: bool = False,
        provider: Optional[str] = None,
    ):
        self._token_audience = token_audience
        self._type_adapter = None

        super().__init__(
            authorization_provider=authorization_provider,
            create_node_endpoint=create_node_endpoint,
            allowed_tags_endpoint=allowed_tags_endpoint,
            scopes_endpoint=scopes_endpoint,
            provider=provider,
            modify_node_endpoint=modify_node_endpoint,
            empty_access_blob_public=empty_access_blob_public,
        )

    def build_input(
        self,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_blob: Optional[AccessBlob] = None,
    ) -> str:
        _input = {"audience": self._token_audience}

        if (
            principal.type is PrincipalType.external
            and principal.access_token is not None
        ):
            _input["token"] = principal.access_token.get_secret_value()

        if access_blob is not None:
            _input.update(access_blob)
        return json.dumps({"input": _input})
