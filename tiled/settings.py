import os
import secrets
from abc import ABC
from datetime import timedelta
from typing import Any, List, Optional, Union

from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass
from pydantic_settings import BaseSettings, CliApp, CliSubCommand, SettingsConfigDict

from tiled.client.context import Context
from tiled.commandline._utils import get_profile

_PROFILE = "Specify an instance of Tiled if configured with multiple."


class Login(BaseModel):
    """
    Log in to an authenticated Tiled server.
    """

    profile: Optional[str] = Field(None, description=_PROFILE)
    show_secret_tokens: bool = Field(
        False, description="Show secret tokens after successful login."
    )

    def cli_cmd(self):
        profile_name, profile_content = get_profile(self.profile)
        options = {"verify": profile_content.get("verify", True)}
        context, _ = Context.from_any_uri(profile_content["uri"], **options)
        context.authenticate()
        if self.show_secret_tokens:
            import json

            print(json.dumps(dict(context.tokens), indent=4))


class Logout(BaseModel):
    """
    Log out.
    """

    profile: Optional[str] = Field(None, description=_PROFILE)

    def cli_cmd(self):
        profile_name, profile_content = get_profile(self.profile)
        context, _ = Context.from_any_uri(
            profile_content["uri"], verify=profile_content.get("verify", True)
        )
        if context.use_cached_tokens():
            context.logout()


class WhoAmI(BaseModel):
    """
    Show logged in identity.
    """

    profile: Optional[str] = Field(description="Specify an instance of Tiled server")

    def cli_cmd(self):
        profile_name, profile_content = get_profile(self.profile)
        options = {"verify": profile_content.get("verify", True)}
        context, _ = Context.from_any_uri(profile_content["uri"], **options)
        context.use_cached_tokens()
        whoami = context.whoami()
        if whoami is None:
            print("Not authenticated.")
        else:
            print(",".join(identity["id"] for identity in whoami["identities"]))


class Server:
    ...


class API(BaseModel, ABC):
    enabled: bool = False

    def serve_api(server: Server):
        ...


class FastAPI(API):
    ...


class GraphQL(API):
    ...


# hashable cache key for use in tiled.authn_database.connection_pool
@dataclass(unsafe_hash=True)
class DatabaseSettings:
    uri: Optional[str] = None
    pool_size: int = 5
    pool_pre_ping: bool = True
    max_overflow: int = 5


class Serve(BaseModel):
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

    apis: list[API] = [FastAPI(enabled=True)]

    model_config = SettingsConfigDict(
        env_prefix="TILED_", nested_model_default_partial_update=True
    )

    def cli_cmd(self) -> None:
        server = None
        for api in self.apis:
            api.serve_api(server)


class Settings(BaseSettings):
    command: CliSubCommand[Union[Login, Logout, WhoAmI, Serve]]

    model_config = SettingsConfigDict(
        env_prefix="TILED_",
        nested_model_default_partial_update=True,
        cli_prog_name="tiled",
        cli_parse_args=True,
        cli_exit_on_error=True,
    )

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)
