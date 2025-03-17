from pydantic import BaseModel
from pydantic_settings import CliApp, CliSubCommand

from tiled.commandline._api_key import APIKeys
from tiled.commandline._database import Database
from tiled.commandline._principal import Principals


class Admin(BaseModel):
    database: CliSubCommand[Database]
    api_keys: CliSubCommand[APIKeys]
    principals: CliSubCommand[Principals]

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)
