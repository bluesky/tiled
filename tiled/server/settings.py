import os
import secrets
from datetime import timedelta
from functools import cache
from typing import Any, List, Optional

from pydantic import Field
from pydantic.dataclasses import dataclass
from pydantic_settings import BaseSettings, SettingsConfigDict


# hashable cache key for use in tiled.authn_database.connection_pool
@dataclass(unsafe_hash=True)
class DatabaseSettings:
    uri: Optional[str] = None
    pool_size: int = 5
    pool_pre_ping: bool = True
    max_overflow: int = 5


class Settings(BaseSettings):
    tree: Any = None
    allow_anonymous_access: bool = False
    allow_origins: List[str] = Field(default_factory=list)
    authenticator: Any = None
    # These 'single user' settings are only applicable if authenticator is None.
    single_user_api_key: str = secrets.token_hex(32)
    single_user_api_key_generated: bool = "TILED_SINGLE_USER_API_KEY" not in os.environ
    # The TILED_SERVER_SECRET_KEYS may be a single key or a ;-separated list of
    # keys to support key rotation. The first key will be used for encryption. Each
    # key will be tried in turn for decryption.
    secret_keys: List[str] = [secrets.token_hex(32)]
    access_token_max_age: timedelta = 15 * 60  # 15 minutes
    refresh_token_max_age: timedelta = 7 * 24 * 60 * 60  # 7 days
    session_max_age: timedelta = 365 * 24 * 60 * 60  # 365 days
    # Put a fairly low limit on the maximum size of one chunk, keeping in mind
    # that data should generally be chunked. When we implement async responses,
    # we can raise this global limit.
    response_bytesize_limit: int = 300_000_000  # 300 MB
    reject_undeclared_specs: bool = False
    database_settings: DatabaseSettings = Field(DatabaseSettings(), alias="database")
    database_init_if_not_exists: bool = False
    expose_raw_assets: bool = True

    model_config = SettingsConfigDict(
        env_prefix="TILED_", nested_model_default_partial_update=True
    )


@cache
def get_settings() -> Settings:
    return Settings()
