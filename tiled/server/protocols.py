from dataclasses import dataclass
from typing import Protocol

from fastapi import Request


@dataclass
class UserSessionState:
    """Data transfer class to communicate custom session state infromation."""

    user_name: str
    state: dict = None


class UsernamePasswordAuthenticator(Protocol):
    def authenticate(self, username: str, password: str) -> UserSessionState:
        pass


class Authenticator(Protocol):
    def authenticate(self, request: Request) -> UserSessionState:
        pass
