from typing import List, Optional

import typer

from ._utils import get_context

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
    max_note_len = max(len(api_key["note"] or "") for api_key in info["api_keys"])
    if (starting_notes_pad := max_note_len) < len("Note"):
        starting_notes_pad += 4 - max_note_len
    max_scopes_len = max(
        sum(len(scope) for scope in api_key["scopes"]) + len(api_key["scopes"]) - 1
        for api_key in info["api_keys"]
    )
    if (starting_scopes_pad := max_scopes_len) < len("Scopes"):
        starting_scopes_pad += 6 - max_scopes_len
    COLUMNS = (
        f"First 8   Expires at (UTC)     "
        f"Latest activity      Note{' ' * (max_note_len - 4)}    "
        f"Scopes{' ' * (max_scopes_len - 6)}    Access tags"
    )
    typer.echo(COLUMNS)
    for api_key in info["api_keys"]:
        note_padding = 4 + starting_notes_pad - len(api_key["note"] or "")
        # the '1' subtraction works in all cases because the amount of spaces
        #   in a sentence is count(words) - 1, and also because we otherwise
        #   print a single 'dash' for an empty list
        scopes_padding = (
            4
            + starting_scopes_pad
            - sum(len(scope) for scope in api_key["scopes"])
            + len(api_key["scopes"])
            - 1
        )
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
        access_tags = (
            " ".join([tag.replace(" ", "\\ ") for tag in api_key["access_tags"]])
            if api_key["access_tags"] is not None
            else "-"
        )
        typer.echo(
            (
                f"{api_key['first_eight']:10}"
                f"{expiration_time:21}"
                f"{latest_activity:21}"
                f"{(api_key['note'] or '')}{' ' * note_padding}"
                f"{' '.join(api_key['scopes']) or '-'}{' ' * scopes_padding}"
                f"{access_tags}"
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
