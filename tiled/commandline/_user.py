import json
from typing import Annotated

from pydantic import BaseModel
from pydantic_settings import CliApp, CliSubCommand

from tiled.commandline._utils import ContextCommand


class Logout(ContextCommand):
    """
    Log out of an authenticated Tiled server.
    """

    def cli_cmd(self) -> None:
        context = self.get_profile_context()
        if context.use_cached_tokens():
            context.logout()


class WhoAmi(ContextCommand):
    """
    Show logged in identity.
    """

    def cli_cmd(self) -> None:
        context = self.get_profile_context()
        context.use_cached_tokens()
        whoami = context.whoami()
        if whoami is None:
            print("Not authenticated.")
        else:
            print(",".join(identity["id"] for identity in whoami["identities"]))


class Login(ContextCommand):
    """
    Log in to an authenticated Tiled server.
    """

    show_secret_tokens: Annotated[
        bool, "Show secret tokens after successful login."
    ] = False

    def cli_cmd(self) -> None:
        context = self.get_profile_context()
        context.authenticate()
        if self.show_secret_tokens:
            print(json.dumps(dict(context.tokens), indent=4))


class User(BaseModel):
    login: CliSubCommand[Login]
    logout: CliSubCommand[Logout]
    whoami: CliSubCommand[WhoAmi]

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)
