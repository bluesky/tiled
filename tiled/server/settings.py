from datetime import timedelta
from functools import lru_cache
import os
import secrets
from typing import Any, List, Optional

from pydantic import BaseSettings


if os.getenv("TILED_SESSION_MAX_AGE"):
    DEFAULT_SESSION_MAX_AGE = timedelta(
        sections=int(os.getenv("TILED_SESSION_MAX_AGE"))
    )
else:
    DEFAULT_SESSION_MAX_AGE = None


class Settings(BaseSettings):

    tree: Any = None
    allow_anonymous_access: bool = bool(
        int(os.getenv("TILED_ALLOW_ANONYMOUS_ACCESS", False))
    )
    allow_origins: List[str] = [
        item for item in os.getenv("TILED_ALLOW_ORIGINS", "").split() if item
    ]
    object_cache_available_bytes = float(
        os.getenv("TILED_OBJECT_CACHE_AVAILABLE_BYTES", "0.15")
    )
    object_cache_log_level = os.getenv("TILED_OBJECT_CACHE_LOG_LEVEL", "INFO")
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
    access_token_max_age: timedelta = timedelta(
        seconds=int(os.getenv("TILED_ACCESS_TOKEN_MAX_AGE", 15 * 60))  # 15 minutes
    )
    refresh_token_max_age: timedelta = timedelta(
        seconds=int(
            os.getenv("TILED_REFRESH_TOKEN_MAX_AGE", 7 * 24 * 60 * 60)
        )  # 7 days
    )
    session_max_age: Optional[timedelta] = DEFAULT_SESSION_MAX_AGE  # None


@lru_cache()
def get_settings():
    return Settings()
