from typing import List, Optional

import typer

from tiled.commandline._utils import echo_api_keys_table, get_context

admin_api_key_app = typer.Typer(no_args_is_help=True)


@admin_api_key_app.command("create")
def create_api_key(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    principal_uuid: str = typer.Argument(
        ..., help="UUID identifying Principal to create API key for"
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
            "By default, it will inherit the scopes of the Principal."
        ),
    ),
    access_tags: Optional[List[str]] = typer.Option(
        None,
        help=(
            "Restrict the access available to the API key by listing specific tags. "
            "If set, restrictive access scopes must also be specified (see --scopes). "
            "By default, it will have no limits on access tags."
        ),
    ),
    note: Optional[str] = typer.Option(None, help="Add a note to label this API key."),
):
    """
    Create an API key for a Principal.
    """
    context = get_context(profile)
    if not scopes:
        # This is how typer interprets unspecified scopes.
        # Replace with None to get default scopes.
        scopes = None
    if expires_in and expires_in.isdigit():
        expires_in = int(expires_in)
    info = context.admin.create_api_key(
        principal_uuid,
        expires_in=expires_in,
        scopes=scopes,
        access_tags=access_tags,
        note=note,
    )
    # TODO Print other info to the stderr?
    typer.echo(info["secret"])


@admin_api_key_app.command("list")
def list_api_keys(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    principal_uuid: str = typer.Argument(
        ..., help="UUID identifying Principal to list API keys for"
    ),
):
    """
    List API keys for a Principal.
    """
    context = get_context(profile)
    info = context.admin.show_principal(principal_uuid)
    if not info["api_keys"]:
        typer.echo("No API keys found", err=True)
        return
    echo_api_keys_table(info["api_keys"])


@admin_api_key_app.command("revoke")
def revoke_api_key(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    principal_uuid: str = typer.Argument(
        ..., help="UUID identifying Principal to revoke API key for"
    ),
    first_eight: str = typer.Argument(
        ..., help="First eight characters of API key (or the whole key)"
    ),
):
    """
    Revoke an API key for a Principal.
    """
    context = get_context(profile)
    context.admin.revoke_api_key(principal_uuid, first_eight[:8])
