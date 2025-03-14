import importlib
from abc import ABC
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import Request
from pydantic import BaseModel


@dataclass
class UserSessionState:
    """Data transfer class to communicate custom session state information."""

    user_name: str
    state: dict = None


class Authenticator(BaseModel, ABC):
    confirmation_message: str = ""


class InternalAuthenticator(Authenticator, ABC):
    def authenticate(self, username: str, password: str) -> Optional[UserSessionState]:
        raise NotImplementedError


class ExternalAuthenticator(Authenticator, ABC):
    def authenticate(self, request: Request) -> Optional[UserSessionState]:
        raise NotImplementedError


def _get_authenticator(value: Any) -> Authenticator:
    if isinstance(value, Authenticator):
        return value
    if isinstance(value, dict) and "type" in value:
        qualified_type = value.pop("type")
        if isinstance(qualified_type, type) and issubclass(
            qualified_type, Authenticator
        ):
            return qualified_type(**value)
        split_name = str(qualified_type).split(".")
        type_name = split_name.pop()
        module = importlib.import_module(".".join(split_name))
        if type_name in module:
            return module["type_name"](**value)
        raise KeyError(
            f"Unable to find an Authenticator subclass called {qualified_type}"
        )
    raise KeyError(f"Unable to deserialize Authenticator {value}")
