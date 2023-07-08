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

from ..utils import UNSET

cli_app = typer.Typer()

from ._admin import admin_app  # noqa: E402
from ._api_key import api_key_app  # noqa: E402
from ._catalog import catalog_app  # noqa: E402
from ._profile import profile_app  # noqa: E402
from ._serve import serve_app  # noqa: E402
from ._utils import get_context, get_profile  # noqa E402

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


@cli_app.command("login")
def login(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    set_default: bool = typer.Option(
        True, help="Use this identity as the default for this API."
    ),
    show_secret_tokens: bool = typer.Option(
        False, "--show-secret-tokens", help="Show secret tokens after successful login."
    ),
):
    """
    Log in to an authenticated Tiled server.
    """
    from ..client.context import Context

    profile_name, profile_content = get_profile(profile)
    options = {"verify": profile_content.get("verify", True)}
    context, _ = Context.from_any_uri(profile_content["uri"], **options)
    # Override sticky 'default_identity'.
    # Always prompt user to specify who they want to log in as.
    context.authenticate(username=None, provider=None, set_default=True)
    if show_secret_tokens:
        import json

        typer.echo(json.dumps(dict(context.tokens), indent=4))


@cli_app.command("logout")
def logout(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    username: Optional[str] = typer.Option(None),
    provider: Optional[str] = typer.Option(None),
):
    """
    Log out.
    """
    from ..client.context import Context

    profile_name, profile_content = get_profile(profile)
    context, _ = Context.from_any_uri(
        profile_content["uri"], verify=profile_content.get("verify", True)
    )
    if username is None:
        username = UNSET
    if provider is None:
        provider = UNSET
    context.authenticate(username=username, provider=provider, set_default=False)
    context.logout()


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


main = cli_app


if __name__ == "__main__":
    main()

# This object is used by the auto-generated documentation.
typer_click_object = typer.main.get_command(cli_app)
