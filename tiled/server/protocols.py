from abc import ABC
from dataclasses import dataclass
from typing import Optional

from fastapi import Request


@dataclass
class UserSessionState:
    """Data transfer class to communicate custom session state information."""

    user_name: str
    state: dict = None


class Authenticator(ABC):
    ...


class InternalAuthenticator(Authenticator, ABC):
    def authenticate(self, username: str, password: str) -> Optional[UserSessionState]:
        raise NotImplementedError


class ExternalAuthenticator(Authenticator, ABC):
    def authenticate(self, request: Request) -> Optional[UserSessionState]:
        raise NotImplementedError
