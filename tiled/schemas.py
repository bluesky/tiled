from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, field_validator


class AboutAuthenticationProvider(BaseModel):
    provider: str
    mode: Literal["internal", "external"]
    links: Dict[str, str]
    confirmation_message: Optional[str] = None

    @field_validator("mode", mode="before")
    @classmethod
    def accept_mode_password_as_backcompat_alias_for_internal(cls, value: Any) -> Any:
        if isinstance(value, str) and value == "password":
            value = "internal"
        return value


class AboutAuthenticationLinks(BaseModel):
    whoami: str
    apikey: str
    refresh_session: str
    revoke_session: str
    logout: str


class AboutAuthentication(BaseModel):
    required: bool
    providers: List[AboutAuthenticationProvider]
    links: Optional[AboutAuthenticationLinks] = None


class About(BaseModel):
    api_version: int
    library_version: str
    formats: Dict[str, List[str]]
    aliases: Dict[str, Dict[str, List[str]]]
    queries: List[str]
    authentication: AboutAuthentication
    links: Dict[str, str]
    meta: Dict[str, Any]
