from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from fastapi import Request


@dataclass
class UserSessionState:
    """Data transfer class to communicate custom session state infromation."""

    user_name: str
    state: dict = None
    
 
@runtime_checkable  # Required to be a field on a BaseSettings
class Authenticator(Protocol):
    ...


class PasswordAuthenticator(Authenticator, ABC):
    @abstractmethod
    def authenticate(self, username: str, password: str) -> UserSessionState | None:
        pass


class ExternalAuthenticator(Authenticator, ABC):
    @abstractmethod
    def authenticate(self, request: Request) -> UserSessionState | None:
        pass
