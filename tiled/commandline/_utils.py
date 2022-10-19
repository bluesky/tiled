import typer


def get_profile(name):
    from ..profiles import load_profiles, paths

    profiles = load_profiles()
    if name is None:
        # Use the default profile.
        # Raise if it is not set or if it is set but does not exit.
        user_profiles_dir = paths[-1]
        filepath = user_profiles_dir / ".default"
        if not filepath.is_file():
            typer.echo(
                """No default profile set. Use:

    tiled connect ...

to set a default or else specify a profile for this command using --profile=PROFILE.
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

to set a default or else specify a profile for this command using --profile=PROFILE.
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
    from ..profiles import paths

    user_profiles_dir = paths[-1]
    filepath = user_profiles_dir / ".default"
    if not filepath.is_file():
        return None
    with open(filepath, "r") as file:
        return file.read()
