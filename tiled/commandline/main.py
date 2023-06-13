from typing import Optional

try:
    import typer
except Exception as err:
    raise Exception(
        """
from ._admin import admin_app
from ._api_key import api_key_app
from ._profile import profile_app
from ._serve import serve_app

You are trying to the run the tiled commandline tool but you do not have the
necessary dependencies. It looks like tiled has been installed with
bare-minimum dependencies, possibly via

    pip install tiled

Instead, try:

    pip install tiled[all]  # Note: on a Mac, you may need quotes like 'tiled[all]'.

which installs *everything* you might want. For other options, see:

    https://blueskyproject.io/tiled/tutorials/installation.html
"""
    ) from err

cli_app = typer.Typer()

from ._admin import admin_app  # noqa: E402
from ._api_key import api_key_app  # noqa: E402
from ._catalog import catalog_app  # noqa: E402
from ._profile import profile_app  # noqa: E402
from ._serve import serve_app  # noqa: E402
from ._utils import (  # noqa E402
    CLI_CACHE_DIR,
    get_context,
    get_default_profile_name,
    get_profile,
)

cli_app.add_typer(
    catalog_app, name="catalog", help="Manage a catalog of data to be served by Tiled."
)
cli_app.add_typer(serve_app, name="serve", help="Launch a Tiled server.")
cli_app.add_typer(
    profile_app, name="profile", help="Examine Tiled 'profiles' (client-side config)."
)
cli_app.add_typer(
    api_key_app, name="api_key", help="Create, list, and revoke API keys."
)
cli_app.add_typer(
    admin_app,
    name="admin",
    help="Administrative utilities for managing large deployments.",
)


@cli_app.command("connect")
def connect(
    uri_or_profile: str = typer.Argument(
        ..., help="URI 'http[s]://...' or a profile name."
    ),
    no_verify: bool = typer.Option(False, "--no-verify", help="Skip SSL verification."),
):
    """
    "Connect" to a Tiled server; set it as default.
    """
    from ..client.context import Context
    from ..profiles import list_profiles, load_profiles, paths

    user_profiles_dir = paths[-1]
    if uri_or_profile.startswith("http://") or uri_or_profile.startswith("https://"):
        # This looks like a URI.
        uri = uri_or_profile
        name = "auto"
        Context.from_any_uri(uri, verify=not no_verify)
        user_profiles_dir.mkdir(parents=True, exist_ok=True)
        with open(user_profiles_dir / "auto.yml", "w") as file:
            file.write(
                f"""# This file is managed by the Tiled CLI.
# Any edits made by hand may be discarded.
auto:
  uri: {uri}
  verify: {"true" if not no_verify else "false"}
"""
            )
    else:
        # Is this a profile name?
        if uri_or_profile in list_profiles():
            name = uri_or_profile
            _, profile = load_profiles()[name]
            if "direct" in profile:
                raise ValueError(
                    f"Profile {profile} uses in a direct (in-process) Tiled server "
                    "and cannot be connected to from the CLI."
                )
            options = {"verify": profile.get("verify", True)}
            if no_verify:
                options["verify"] = False
            Context.from_any_uri(profile["uri"], **options)
        else:
            typer.echo(
                f"Not sure what to do with tree {uri_or_profile!r}. "
                "It does not look like a URI (it does not start with http[s]://) "
                "and it does not match any profiles. Use `tiled profiles list` to "
                "see profiles.",
                err=True,
            )
            raise typer.Abort()
    CLI_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CLI_CACHE_DIR / "active_profile", "w") as file:
        file.write(name)
    typer.echo(f"Tiled profile {name!r} is set as the default.")


@cli_app.command("status")
def status():
    """
    Show the current default Tiled server.
    """
    name = get_default_profile_name()
    if name is None:
        typer.echo("Not connected.", err=True)
    else:
        typer.echo(f"Using profile {name!r}\n", err=True)
        import yaml

        _, profile_content = get_profile(name)
        typer.echo(yaml.dump(profile_content))


@cli_app.command("disconnect")
def disconnect():
    """
    "Disconnect" from the default Tiled server.
    """
    filepath = CLI_CACHE_DIR / "active_profile"
    # filepath.unlink(missing_ok=False)  # Python 3.8+
    try:
        filepath.unlink()
    except FileNotFoundError:
        pass


@cli_app.command("login")
def login(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    show_secret_tokens: bool = typer.Option(
        False, "--show-secret-tokens", help="Show secret tokens after successful login."
    ),
):
    """
    Log in to an authenticated Tiled server.
    """
    import json

    from ..client.context import Context

    profile_name, profile_content = get_profile(profile)
    options = {"verify": profile_content.get("verify", True)}
    context, _ = Context.from_any_uri(profile_content["uri"], **options)
    provider_spec, username = context.authenticate()
    filepath = CLI_CACHE_DIR / "profile_auths"
    filepath.mkdir(parents=True, exist_ok=True)
    with open(filepath / profile_name, "w") as file:
        json.dump({"provider": provider_spec["provider"], "username": username}, file)
    if show_secret_tokens:
        typer.echo(json.dumps(dict(context.tokens), indent=4))


@cli_app.command("logout")
def logout(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
):
    """
    Log out from one or all authenticated Tiled servers.
    """
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
    context.logout()
    # filepath.unlink(missing_ok=False)  # Python 3.8+
    try:
        filepath.unlink()
    except FileNotFoundError:
        pass


@cli_app.command("tree")
def tree(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    max_lines: int = typer.Argument(20, help="Max lines to show."),
):
    """
    Show the names of entries in a Tree.

    This is similar to the UNIX utility `tree` for listing nested directories.
    """
    from ..client.constructors import from_context
    from ..utils import gen_tree

    context = get_context(profile)
    client = from_context(context)
    for counter, line in enumerate(gen_tree(client), start=1):
        if (max_lines is not None) and (counter > max_lines):
            print(
                f"Output truncated at {max_lines} lines. "
                "Use `tiled tree <N>` to see <N> lines."
            )
            break
        print(line)


@cli_app.command("download")
def download(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    cache_path: str = typer.Argument(..., help="Local directory for cache storage"),
    capacity: Optional[int] = typer.Argument(None, help="Max capacity in bytes"),
):
    """
    Download content from a Tree to an on-disk cache.
    """
    from ..client.cache import Cache, download
    from ..client.constructors import from_profile

    profile_name, _ = get_profile(profile)

    cache = Cache.on_disk(cache_path, capacity=capacity)
    client = from_profile(profile_name, cache=cache)
    download(client)


main = cli_app


if __name__ == "__main__":
    main()

# This object is used by the auto-generated documentation.
typer_click_object = typer.main.get_command(cli_app)
