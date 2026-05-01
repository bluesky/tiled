from typing import List, Optional

import typer

from ._utils import echo_api_keys_table, get_context

api_key_app = typer.Typer(no_args_is_help=True)


@api_key_app.command("create")
def create_api_key(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    expires_in: Optional[str] = typer.Option(
        None,
        help=(
            "Number of seconds until API key expires, given as integer seconds "
            "or a string like: '3y' (years), '3d' (days), '5m' (minutes), '1h' "
            "(hours), '30s' (seconds). If None, it will never expire or it will "
            "have the maximum lifetime allowed by the server. "
        ),
    ),
    scopes: Optional[List[str]] = typer.Option(
        None,
        help=(
            "Restrict the access available to this API key by listing scopes. "
            "By default, it will inherit the scopes of its owner."
        ),
    ),
    access_tags: Optional[List[str]] = typer.Option(
        None,
        help=(
            "Restrict the access available to the API key by listing specific tags. "
            "By default, it will have no limits on access tags."
        ),
    ),
    note: Optional[str] = typer.Option(None, help="Add a note to label this API key."),
    no_verify: bool = typer.Option(False, "--no-verify", help="Skip SSL verification."),
):
    context = get_context(profile)
    if not scopes:
        # This is how typer interprets unspecified scopes.
        # Replace with None to get default scopes.
        scopes = None
    if expires_in and expires_in.isdigit():
        expires_in = int(expires_in)
    info = context.create_api_key(
        scopes=scopes, access_tags=access_tags, expires_in=expires_in, note=note
    )
    # TODO Print other info to the stderr?
    typer.echo(info["secret"])


@api_key_app.command("list")
def list_api_keys(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
):
    context = get_context(profile)
    info = context.whoami()
    if not info["api_keys"]:
        typer.echo("No API keys found", err=True)
        return
    echo_api_keys_table(info["api_keys"])


@api_key_app.command("revoke")
def revoke_api_key(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    first_eight: str = typer.Argument(
        ..., help="First eight characters of API key (or the whole key)"
    ),
):
    context = get_context(profile)
    context.revoke_api_key(first_eight[:8])
