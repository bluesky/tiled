from dataclasses import dataclass
from typing import Protocol


@dataclass
class UserSessionState:
    """Data transfer class to communicate custom state infromation."""

    user_name: str
    state: dict = None


class UsernamePasswordAuthenticator(Protocol):
    def authenticate(self, username: str, password: str) -> UserSessionState:
        pass
