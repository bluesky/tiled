import os
import sys
from asyncio import subprocess
from typing import Annotated, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import CliApp, CliSubCommand

from tiled.commandline._utils import ContextCommand
from tiled.profiles import (
    ProfileExists,
    create_profile,
    delete_profile,
    get_default_profile_name,
    load_profiles,
    paths,
    set_default_profile_name,
)


class Paths(BaseModel):
    """
    List the locations that the client will search for profiles (client-side configuration).
    """

    def cli_cmd(self) -> None:
        print("\n".join(str(p) for p in paths))


class ListProfiles(BaseModel):
    """List the profiles (client-side configuration) found and the files they were read from."""

    def cli_cmd(self) -> None:
        profiles = load_profiles()
        if not profiles:
            print("No profiles found.")
            return
        max_len = max(len(name) for name in profiles)
        PADDING = 4

        print(
            "\n".join(
                f"{name:<{max_len + PADDING}}{filepath}"
                for name, (filepath, _) in profiles.items()
            )
        )


class Show(ContextCommand):
    "Show the content of a profile."

    def cli_cmd(self) -> None:
        filepath, content = self.profile_contents()
        print(f"Source: {filepath}", file=sys.stderr)
        print("--", file=sys.stderr)
        print(yaml.dump(content), file=sys.stdout)


class Edit(ContextCommand):
    def cli_cmd(self) -> None:
        filepath, _ = self.profile_contents()
        print(f"Opening {filepath} in default text editor...", file=sys.stderr)

        if sys.platform.system() == "Darwin":
            subprocess.call(("open", filepath))
        elif sys.platform.system() == "Windows":
            os.startfile(filepath)
        else:
            subprocess.call(("xdg-open", filepath))


class Create(ContextCommand):
    uri: Annotated[str, "URI 'http[s]://...'"]
    set_default: Annotated[bool, "Set new profile as the default profile."] = True
    overwrite: Annotated[bool, "Overwrite an existing profile of this name."] = False
    verify: Annotated[bool, "Perform SSL verification."] = True
    """
    Create a 'profile' that can be used to connect to a Tiled server.
    """

    def cli_cmd(self):
        try:
            create_profile(
                name=self.profile_name,
                uri=self.uri,
                verify=self.verify,
                overwrite=self.overwrite,
            )
        except ProfileExists as e:
            print(
                f"A profile named {self.profile_name!r} already exists. Use --overwrite to overwrite it."
            )
            raise e
        if self.set_default:
            set_default_profile_name(self.profile_name)
            print(
                f"Tiled profile {self.profile_name!r} created and set as the default."
            )
        else:
            print(f"Tiled profile {self.profile_name!r} created.")


class Delete(ContextCommand):
    def cli_cmd(self) -> None:
        # Unset the default if this profile is currently the default.
        default = get_default_profile_name()
        if default == self.profile_name:
            set_default_profile_name(None)
        delete_profile(self.profile_name)
        print(f"Tiled profile {self.profile_name!r} deleted.")


class GetDefault(BaseModel):
    """
    Show the current default Tiled profile.
    """

    def cli_cmd(self) -> None:
        name = get_default_profile_name()
        if name is None:
            print("No default.")
        else:
            source_filepath, profile_content = load_profiles()[name]
            print(f"# Profile name: {name!r} # {source_filepath} \n")
            print(profile_content)


class SetDefault(BaseModel):
    """
    Set the default Tiled profile.
    """

    profile_name: Annotated[
        Optional[str], "Profile name to set as default, or None to clear"
    ] = None

    def cli_cmd(self) -> None:
        set_default_profile_name(self.profile_name)


class DefaultProfile(BaseModel):
    set_default: CliSubCommand[SetDefault] = Field(alias="set")
    get: CliSubCommand[GetDefault]

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)


class Profiles(BaseModel):
    default_profile: CliSubCommand[DefaultProfile] = Field(alias="default")
    paths: CliSubCommand[Paths]
    list_profiles: CliSubCommand[ListProfiles] = Field(alias="list")
    show: CliSubCommand[Show]
    edit: CliSubCommand[Edit]
    create: CliSubCommand[Create]
    delete: CliSubCommand[Delete]

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)
