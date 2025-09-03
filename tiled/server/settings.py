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
    """A BaseSettings object defining configuration for the tiled instance.
    For loading variables from the environment, prefix with TILED_ and see:
    https://docs.pydantic.dev/latest/concepts/pydantic_settings/#parsing-environment-variable-values
    """

    tree: Any = None
    allow_anonymous_access: bool = False
    allow_origins: List[str] = Field(default_factory=list)
    authenticator: Any = None
    # These 'single user' settings are only applicable if authenticator is None.
    single_user_api_key: str = secrets.token_hex(32)
    # The first key will be used for encryption. Each key will be tried in turn for decryption.
    secret_keys: List[str] = [secrets.token_hex(32)]
    access_token_max_age: timedelta = timedelta(minutes=15)
    refresh_token_max_age: timedelta = timedelta(days=7)
    session_max_age: timedelta = timedelta(days=365)
    # Put a fairly low limit on the maximum size of one chunk, keeping in mind
    # that data should generally be chunked. When we implement async responses,
    # we can raise this global limit.
    response_bytesize_limit: int = 300_000_000  # 300 MB
    # The largest number of items in a container for which the metadata endpoint
    # will return the exact count; this becomes the lower bound on the stimate if
    # an approximate number can not be obtained otherwise.
    exact_count_limit: int = 100
    reject_undeclared_specs: bool = False
    # "env_prefix does not apply to fields with alias"
    # https://docs.pydantic.dev/latest/concepts/pydantic_settings/#environment-variable-names
    database_settings: DatabaseSettings = Field(
        DatabaseSettings(), validation_alias="TILED_DATABASE"
    )
    # Connection pool configurations for catalog and storage DBs
    catalog_pool_size: int = 5
    storage_pool_size: int = 5
    catalog_max_overflow: int = 10
    storage_max_overflow: int = 10
    database_init_if_not_exists: bool = False
    expose_raw_assets: bool = True

    model_config = SettingsConfigDict(
        env_prefix="TILED_",
        nested_model_default_partial_update=True,
        env_nested_delimiter="_",
    )


@cache
def get_settings() -> Settings:
    return Settings()
