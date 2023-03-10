from typing import List, Optional

import typer

from ._utils import get_context

api_key_app = typer.Typer()


@api_key_app.command("create")
def create_api_key(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    expires_in: Optional[int] = typer.Option(
        None,
        help=(
            "Number of seconds until API key expires. If None, "
            "it will never expire or it will have the maximum lifetime "
            "allowed by the server."
        ),
    ),
    scopes: Optional[List[str]] = typer.Option(
        None,
        help=(
            "Restrict the access available to this API key by listing scopes. "
            "By default, it will inherit the scopes of its owner."
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
    info = context.create_api_key(scopes=scopes, expires_in=expires_in, note=note)
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
    max_note_len = max(len(api_key["note"] or "") for api_key in info["api_keys"])
    COLUMNS = f"First 8   Expires at (UTC)     Latest activity      Note{' ' * (max_note_len - 4)}  Scopes"
    typer.echo(COLUMNS)
    for api_key in info["api_keys"]:
        note_padding = 2 + max_note_len - len(api_key["note"] or "")
        if api_key["expiration_time"] is None:
            expiration_time = "-"
        else:
            expiration_time = (
                api_key["expiration_time"]
                .replace(microsecond=0, tzinfo=None)
                .isoformat()
            )
        if api_key["latest_activity"] is None:
            latest_activity = "-"
        else:
            latest_activity = (
                api_key["latest_activity"]
                .replace(microsecond=0, tzinfo=None)
                .isoformat()
            )
        typer.echo(
            (
                f"{api_key['first_eight']:10}"
                f"{expiration_time:21}"
                f"{latest_activity:21}"
                f"{(api_key['note'] or '')}{' ' * note_padding}"
                f"{' '.join(api_key['scopes']) or '-'}"
            )
        )


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
