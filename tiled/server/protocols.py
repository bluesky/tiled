from dataclasses import dataclass
from typing import Protocol

@dataclass
class UserSessionState():
    user_name: str
    state: dict = None


class UsernamePasswordAuthenticator(Protocol):
    
    def authenticate(self, username: str, password: str) -> UserSessionState:
        pass