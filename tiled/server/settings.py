import secrets
from datetime import timedelta
from functools import cache
from typing import Annotated, Any, List, Optional

from pydantic import BaseModel, BeforeValidator, Field
from pydantic.dataclasses import dataclass
from pydantic_settings import BaseSettings, SettingsConfigDict

from tiled.server.protocols import Authenticator, _get_authenticator


class Admin(BaseModel):
    provider: str
    id: str


class AuthenticatorInfo(BaseModel):
    provider: str
    authenticator: Annotated[Authenticator, BeforeValidator(_get_authenticator)]


class UnscalableConfig(Exception):
    pass


# hashable cache key for use in tiled.authn_database.connection_pool
@dataclass(unsafe_hash=True)
class DatabaseSettings:
    uri: Optional[str] = None
    pool_size: int = 5
    pool_pre_ping: bool = True
    max_overflow: int = 5


class Uvicorn(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8000


class Settings(BaseSettings):
    """A BaseSettings object defining configuration for the tiled instance.
    For loading variables from the environment, prefix with TILED_ and see:
    https://docs.pydantic.dev/latest/concepts/pydantic_settings/#parsing-environment-variable-values
    """

    tree: Any = None
    allow_anonymous_access: bool = False
    allow_origins: List[str] = Field(default_factory=list)
    authenticators: list[AuthenticatorInfo] = Field(default_factory=list)
    # These 'single user' settings are only applicable if authenticator is None.
    single_user_api_key: str = secrets.token_hex(32)
    # The first key will be used for encryption. Each key will be tried in turn for decryption.
    secret_keys: List[str] = [secrets.token_hex(32)]
    access_token_max_age: timedelta = 15 * 60  # 15 minutes
    refresh_token_max_age: timedelta = 7 * 24 * 60 * 60  # 7 days
    session_max_age: timedelta = 365 * 24 * 60 * 60  # 365 days
    # Put a fairly low limit on the maximum size of one chunk, keeping in mind
    # that data should generally be chunked. When we implement async responses,
    # we can raise this global limit.
    response_bytesize_limit: int = 300_000_000  # 300 MB
    reject_undeclared_specs: bool = False
    # "env_prefix does not apply to fields with alias"
    # https://docs.pydantic.dev/latest/concepts/pydantic_settings/#environment-variable-names
    database_settings: DatabaseSettings = Field(
        DatabaseSettings(), validation_alias="TILED_DATABASE"
    )
    database_init_if_not_exists: bool = False
    expose_raw_assets: bool = True
    admins: list[Admin] = Field(default_factory=list)

    uvicorn: Uvicorn = Field(Uvicorn())

    model_config = SettingsConfigDict(
        env_prefix="TILED_",
        nested_model_default_partial_update=True,
        env_nested_delimiter="_",
    )

    def check_scalable(self):
        if self.authenticators:
            if not self.secret_keys:
                raise UnscalableConfig(
                    "In a multi-process deployment configured with Authenticator(s), secret keys must be provided"
                )
            # Multi-user authentication requires a database. We cannot fall
            # back to the default of an in-memory SQLite database in a
            # horizontally scaled deployment.
            if not self.database_settings.uri:
                raise UnscalableConfig(
                    "In a multi-process deployment configured with Authenticator(s) a database must be provided"
                )
        else:
            # No authentication provider is configured, so no secret keys are
            # needed, but a single-user API key must be set.
            if not self.single_user_api_key:
                raise UnscalableConfig(
                    "In a multi-process deployment without an Authenticator configured an API key must be provided"
                )


@cache
def get_settings() -> Settings:
    return Settings()
