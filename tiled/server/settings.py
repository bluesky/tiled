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
    # The TILED_SERVER_SECRET_KEYS may be a single key or a ;-separated list of
    # keys to support key rotation. The first key will be used for encryption. Each
    # key will be tried in turn for decryption.
    secret_keys: List[str] = os.getenv(
        "TILED_SERVER_SECRET_KEYS", secrets.token_hex(32)
    ).split(";")


@lru_cache()
def get_settings():
    return Settings()
