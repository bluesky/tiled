from functools import lru_cache
import os
import secrets
from typing import Any, List

from pydantic import BaseSettings


class Settings(BaseSettings):

    catalog: Any = None
    allow_anonymous_access: bool = bool(
        int(os.getenv("TILED_ALLOW_ANONYMOUS_ACCESS", True))
    )
    allow_origins: List[str] = [
        item for item in os.getenv("TILED_ALLOW_ORIGINS", "").split() if item
    ]
    authenticator: Any = None
    # These 'single user' settings are only applicable if authenticator is None.
    single_user_api_key = os.getenv("TILED_SINGLE_USER_API_KEY", secrets.token_hex(32))
    single_user_api_key_generated = not ("TILED_SINGLE_USER_API_KEY" in os.environ)


@lru_cache()
def get_settings():
    return Settings()
