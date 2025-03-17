import json
from typing import Annotated

from pydantic import BaseModel, Field
from pydantic_settings import CliApp, CliSubCommand

from tiled.commandline._utils import ContextCommand


class ListPrincipals(ContextCommand):
    page_offset: int = 0
    page_limit: Annotated[int, "Max items to show"] = 100
    """
    List information about all Principals (users or services) that have ever logged in.
    """

    def cli_cmd(self) -> None:
        context = self.context()
        result = context.admin.list_principals(
            offset=self.page_offset, limit=self.page_limit
        )
        print(json.dumps(result, indent=2))


class Show(ContextCommand):
    uuid: Annotated[str, "UUID identifying Principal of interest"]
    """
    Show information about one Principal (user or service).
    """

    def cli_cmd(self) -> None:
        context = self.context()
        result = context.admin.show_principal(self.uuid)
        print(json.dumps(result, indent=2))


class Principals(BaseModel):
    list_principals: CliSubCommand[ListPrincipals] = Field(alias="list")
    show: CliSubCommand[Show]

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)
