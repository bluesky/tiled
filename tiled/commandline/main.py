from typing import Annotated

from pydantic_settings import BaseSettings, CliApp, CliSubCommand, SettingsConfigDict

from tiled.client.constructors import from_context
from tiled.commandline._user import User
from tiled.commandline._utils import ContextCommand
from tiled.utils import gen_tree

from ._admin import Admin
from ._profile import Profiles
from ._register import Register
from ._serve import Serve


class Tree(ContextCommand):
    max_lines: Annotated[int, "Max lines to show."] = 20
    """
    Show the names of entries in a Tree.

    This is similar to the UNIX utility `tree` for listing nested directories.
    """

    def cli_cmd(self) -> None:
        context = self.context()
        client = from_context(context)
        for counter, line in enumerate(gen_tree(client), start=1):
            if counter > self.max_lines:
                print(
                    f"Output truncated at {self.max_lines} lines. "
                    "Use `tiled tree <N>` to see <N> lines."
                )
                break
            print(line)


class Tiled(BaseSettings, cli_parse_args=True):
    profiles: CliSubCommand[Profiles]
    user: CliSubCommand[User]
    admin: CliSubCommand[Admin]
    serve: CliSubCommand[Serve]
    register: CliSubCommand[Register]
    tree: CliSubCommand[Tree]

    model_config = SettingsConfigDict(cli_parse_args=True)

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)
