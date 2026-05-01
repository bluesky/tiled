from typing import List

import typer


def get_profile(name):
    from ..profiles import (
        get_default_profile_name,
        load_profiles,
        set_default_profile_name,
    )

    profiles = load_profiles()
    if name is None:
        # Use the default profile.
        # Raise if it is not set or if it is set but does not exit.
        name = get_default_profile_name()
        if name is None:
            typer.echo(
                """No default profile set. Use:

    tiled profile create ...

or

    tiled profile set-default ...

to set a create or choose a default profile or else specify a profile for this
particular command using

    tiled ... --profile=PROFILE
""",
                err=True,
            )
            raise typer.Abort()
        if name not in profiles:
            set_default_profile_name(None)
            typer.echo(
                f"""Default profile {name!r} does not exist. Clearing default. Use:

    tiled profile create ...

or

    tiled profile set-default ...

to set a create or choose a default profile or else specify a profile for this
particular command using

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


def get_context(profile):
    from ..client.context import Context

    profile_name, profile_content = get_profile(profile)
    context, _ = Context.from_any_uri(
        profile_content["uri"], verify=profile_content.get("verify", True)
    )
    if not context.use_cached_tokens():
        if context.server_info.authentication.required:
            context.authenticate()
    return context


def echo_api_keys_table(api_keys: List[dict]):
    max_note_len = max(len(api_key["note"] or "") for api_key in api_keys)
    if (starting_notes_pad := max_note_len) < len("Note"):
        starting_notes_pad += 4 - max_note_len
    max_scopes_len = max(
        sum(len(scope) for scope in api_key["scopes"]) + len(api_key["scopes"]) - 1
        for api_key in api_keys
    )
    if (starting_scopes_pad := max_scopes_len) < len("Scopes"):
        starting_scopes_pad += 6 - max_scopes_len
    COLUMNS = (
        f"First 8   Expires at (UTC)     "
        f"Latest activity      Note{' ' * (max_note_len - 4)}    "
        f"Scopes{' ' * (max_scopes_len - 6)}    Access tags"
    )
    typer.echo(COLUMNS)
    for api_key in api_keys:
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
