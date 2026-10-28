from typing import Any, Literal

from pydantic import BaseModel


class AboutAuthenticationProvider(BaseModel):
    provider: str
    # "password" is a back-compat alias for "internal"; "internal should be preferred.
    mode: Literal["internal", "external", "password"]
    links: dict[str, str]
    confirmation_message: str | None = None
    extra_scopes: list[str] | None = None


class AboutAuthenticationLinks(BaseModel):
    whoami: str
    apikey: str
    refresh_session: str
    revoke_session: str
    logout: str


class AboutAuthentication(BaseModel):
    required: bool
    providers: list[AboutAuthenticationProvider]
    links: AboutAuthenticationLinks | None = None


class About(BaseModel):
    api_version: int
    library_version: str
    formats: dict[str, list[str]]
    aliases: dict[str, dict[str, list[str]]]
    queries: list[str]
    authentication: AboutAuthentication
    links: dict[str, str]
    meta: dict[str, Any]
