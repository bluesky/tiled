import os
from pathlib import Path

import appdirs
import typer

CLI_CACHE_DIR = Path(
    os.getenv(
        "TILED_CLI_CACHE_DIR", os.path.join(appdirs.user_cache_dir("tiled"), "cli")
    )
)


def get_profile(name):
    from ..profiles import load_profiles

    profiles = load_profiles()
    if name is None:
        # Use the default profile.
        # Raise if it is not set or if it is set but does not exit.
        filepath = CLI_CACHE_DIR / "active_profile"
        if not filepath.is_file():
            typer.echo(
                """No default profile set. Use:

    tiled connect ...

to set a default profile or else specify a profile for this command using

    tiled ... --profile=PROFILE
""",
                err=True,
            )
            raise typer.Abort()
        with open(filepath, "r") as file:
            name = file.read()
        if name not in profiles:
            filepath.unlink()
            typer.echo(
                f"""Default profile {name!r} does not exist. Clearing default. Use:

    tiled connect ...

to set a default profile or else specify a profile for this command using

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


def get_default_profile_name():
    filepath = CLI_CACHE_DIR / "active_profile"
    if not filepath.is_file():
        return None
    with open(filepath, "r") as file:
        return file.read()


def get_context(profile):
    import json

    from ..client.context import Context

    profile_name, profile_content = get_profile(profile)
    filepath = CLI_CACHE_DIR / "profile_auths" / profile_name
    context, _ = Context.from_any_uri(
        profile_content["uri"], verify=profile_content.get("verify", True)
    )
    if filepath.is_file():
        with open(filepath, "r") as file:
            auth = json.load(file)
        context.authenticate(auth["username"], auth["provider"])
    return context
