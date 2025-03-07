from typing import Optional, Union

from pydantic import BaseModel, Field
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


class Settings(BaseSettings):
    command: CliSubCommand[Union[Login, Logout, WhoAmI]]

    model_config = SettingsConfigDict(
        env_prefix="TILED_",
        nested_model_default_partial_update=True,
        cli_prog_name="tiled",
        cli_parse_args=True,
        cli_exit_on_error=True,
    )

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)
