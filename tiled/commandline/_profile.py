import typer

profile_app = typer.Typer()


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
