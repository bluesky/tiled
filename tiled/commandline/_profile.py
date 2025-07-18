import typer

profile_app = typer.Typer(no_args_is_help=True)


@profile_app.command("paths")
def profile_paths():
    "List the locations that the client will search for profiles (client-side configuration)."
    from ..profiles import paths

    print("\n".join(str(p) for p in paths))


@profile_app.command("list")
def profile_list():
    "List the profiles (client-side configuration) found and the files they were read from."
    from ..profiles import load_profiles

    profiles = load_profiles()
    if not profiles:
        typer.echo("No profiles found.")
        return
    max_len = max(len(name) for name in profiles)
    PADDING = 4

    print(
        "\n".join(
            f"{name:<{max_len + PADDING}}{filepath}"
            for name, (filepath, _) in profiles.items()
        )
    )


@profile_app.command("show")
def profile_show(profile_name: str):
    "Show the content of a profile."
    import sys

    import yaml

    from ..profiles import load_profiles

    profiles = load_profiles()
    try:
        filepath, content = profiles[profile_name]
    except KeyError:
        typer.echo(
            f"The profile {profile_name!r} could not be found. "
            "Use tiled profile list to see profile names.",
            err=True,
        )
        raise typer.Abort()
    print(f"Source: {filepath}", file=sys.stderr)
    print("--", file=sys.stderr)
    print(yaml.dump(content), file=sys.stdout)


@profile_app.command("edit")
def profile_edit(profile_name: str):
    "Show the content of a profile."
    import sys

    from ..profiles import load_profiles

    profiles = load_profiles()
    try:
        filepath, content = profiles[profile_name]
    except KeyError:
        typer.echo(
            f"The profile {profile_name!r} could not be found. "
            "Use tiled profile list to see profile names.",
            err=True,
        )
        raise typer.Abort()
    print(f"Opening {filepath} in default text editor...", file=sys.stderr)

    import os
    import platform
    import subprocess

    if platform.system() == "Darwin":
        subprocess.call(("open", filepath))
    elif platform.system() == "Windows":
        os.startfile(filepath)
    else:
        subprocess.call(("xdg-open", filepath))


@profile_app.command("create")
def create(
    uri: str = typer.Argument(..., help="URI 'http[s]://...'"),
    name: str = typer.Option("auto", help="Profile name, a short convenient alias"),
    set_default: bool = typer.Option(
        True, help="Set new profile as the default profile."
    ),
    overwrite: bool = typer.Option(
        False, "--overwrite", help="Overwrite an existing profile of this name."
    ),
    no_verify: bool = typer.Option(False, "--no-verify", help="Skip SSL verification."),
):
    """
    Create a 'profile' that can be used to connect to a Tiled server.
    """
    from ..profiles import ProfileExists, create_profile, set_default_profile_name

    try:
        create_profile(name=name, uri=uri, verify=not no_verify, overwrite=overwrite)
    except ProfileExists:
        typer.echo(
            f"A profile named {name!r} already exists. Use --overwrite to overwrite it."
        )
        raise typer.Abort()
    if set_default:
        set_default_profile_name(name)
        typer.echo(f"Tiled profile {name!r} created and set as the default.", err=True)
    else:
        typer.echo(f"Tiled profile {name!r} created.", err=True)


@profile_app.command("delete")
def delete(
    name: str = typer.Argument(..., help="Profile name"),
):
    from ..profiles import (
        delete_profile,
        get_default_profile_name,
        set_default_profile_name,
    )

    # Unset the default if this profile is currently the default.
    default = get_default_profile_name()
    if default == name:
        set_default_profile_name(None)
    delete_profile(name)
    typer.echo(f"Tiled profile {name!r} deleted.", err=True)


@profile_app.command("get-default")
def get_default():
    """
    Show the current default Tiled profile.
    """
    from ..profiles import get_default_profile_name, load_profiles

    name = get_default_profile_name()
    if name is None:
        typer.echo("No default.", err=True)
    else:
        import yaml

        source_filepath, profile_content = load_profiles()[name]
        typer.echo(f"# Profile name: {name!r}")
        typer.echo(f"# {source_filepath} \n")
        typer.echo(yaml.dump(profile_content))


@profile_app.command("set-default")
def set_default(profile_name: str):
    """
    Set the default Tiled profile.
    """
    from ..profiles import set_default_profile_name

    set_default_profile_name(profile_name)


@profile_app.command("clear-default")
def clear_default():
    """
    Clear the default Tiled profile.
    """
    from ..profiles import set_default_profile_name

    set_default_profile_name(None)
