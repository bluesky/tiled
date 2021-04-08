from functools import lru_cache
import os
from typing import Any, List

from pydantic import BaseSettings


class DummyAuthenticator:
    def authenticate(self, username: str, password: str):
        return username


class Settings(BaseSettings):

    catalog: Any = None
    allow_anonymous_access: bool = bool(
        int(os.getenv("TILED_ALLOW_ANONYMOUS_ACCESS", True))
    )
    # TODO Should we use PAM by default? Or raise an error by default?
    authenticator: Any = DummyAuthenticator()
    allow_origins: List[str] = [
        item for item in os.getenv("TILED_ALLOW_ORIGINS", "").split() if item
    ]


@lru_cache()
def get_settings():
    return Settings()
