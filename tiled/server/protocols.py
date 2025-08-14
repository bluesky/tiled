from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import Request
from pydantic import BaseModel


@dataclass
class UserSessionState:
    """Data transfer class to communicate custom session state information."""

    user_name: str
    state: Optional[dict[str, Any]] = None


class Authenticator(BaseModel, ABC):
    confirmation_message: str = ""


class InternalAuthenticator(Authenticator):
    @abstractmethod
    async def authenticate(
        self, username: str, password: str
    ) -> Optional[UserSessionState]:
        ...


class ExternalAuthenticator(Authenticator):
    @abstractmethod
    async def authenticate(self, request: Request) -> Optional[UserSessionState]:
        ...
