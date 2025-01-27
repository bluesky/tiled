from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel


class AboutAuthenticationProvider(BaseModel):
    provider: str
    mode: Literal["internal", "external"]
    links: Dict[str, str]
    confirmation_message: Optional[str] = None


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
