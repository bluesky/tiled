from abc import ABC
from pathlib import Path
from typing import Annotated, Any, Optional

import typer
from pydantic import BaseModel

from tiled.client.context import Context
from tiled.profiles import load_profiles


class ContextCommand(ABC, BaseModel):
    profile_name: Annotated[
        Optional[str],
        "Specify a profile to use when talking to one of multiple Tiled instances.",
    ] = None

    def profile_contents(self) -> tuple[Path, dict[str, Any]]:
        profiles = load_profiles()
        if self.profile_name not in profiles:
            raise KeyError(
                f"The profile {self.profile_name!r} could not be found. "
                "Use tiled profile list to see profile names.",
            )
        return profiles[self.profile_name]

    def context(self) -> Context:
        return get_context(self.profile_name)


def get_profile(name):
    from ..profiles import (
        get_default_profile_name,
        load_profiles,
        set_default_profile_name,
    )

    profiles = load_profiles()
    if name is None:
        # Use the default profile.
        # Raise if it is not set or if it is set but does not exit.
        name = get_default_profile_name()
        if name is None:
            typer.echo(
                """No default profile set. Use:

    tiled profile create ...

or

    tiled profile set-default ...

to set a create or choose a default profile or else specify a profile for this
particular command using

    tiled ... --profile=PROFILE
""",
                err=True,
            )
            raise typer.Abort()
        if name not in profiles:
            set_default_profile_name(None)
            typer.echo(
                f"""Default profile {name!r} does not exist. Clearing default. Use:

    tiled profile create ...

or

    tiled profile set-default ...

to set a create or choose a default profile or else specify a profile for this
particular command using

    tiled ... --profile=PROFILE
""",
                err=True,
            )
            raise typer.Abort()
    try:
        _, profile = profiles[name]
        if "direct" in profile:
            typer.echo(
                f"Profile {profile!r} uses in a direct (in-process) Tiled server "
                "and cannot be connected to from the CLI.",
                err=True,
            )
            typer.Abort()
    except KeyError:
        typer.echo(
            f"""Profile {name!r} could not be found. Use:

    tiled profile list

to list choices.""",
            err=True,
        )
        raise typer.Abort()
    return name, profile


def get_context(profile):
    from ..client.context import Context

    profile_name, profile_content = get_profile(profile)
    context, _ = Context.from_any_uri(
        profile_content["uri"], verify=profile_content.get("verify", True)
    )
    return context
