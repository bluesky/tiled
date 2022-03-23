from pathlib import Path
from typing import List, Optional

import typer

cli_app = typer.Typer()
serve_app = typer.Typer()
profile_app = typer.Typer()
api_key_app = typer.Typer()
admin_app = typer.Typer()
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


@admin_app.command("initialize-database")
def initialize_database(database_uri: str):
    """
    Initialize a SQL database for use by Tiled.
    """
    from sqlalchemy import create_engine

    from ..database.core import (
        REQUIRED_REVISION,
        UninitializedDatabase,
        check_database,
        initialize_database,
    )

    engine = create_engine(database_uri)
    redacted_url = engine.url._replace(password="[redacted]")
    try:
        check_database(engine)
    except UninitializedDatabase:
        # Create tables and stamp (alembic) revision.
        typer.echo(
            f"Database {redacted_url} is new. Creating tables and marking revision {REQUIRED_REVISION}."
        )
        initialize_database(engine)
        typer.echo("Database initialized.")
    else:
        typer.echo(f"Database at {redacted_url} is already initialized.")
        raise typer.Abort()


@admin_app.command("upgrade-database")
def upgrade_database(
    database_uri: str,
    revision: Optional[str] = typer.Argument(
        None,
        help="The ID of a revision to upgrade to. By default, upgrade to the latest one.",
    ),
):
    """
    Upgrade the database schema to the latest version.
    """
    from sqlalchemy import create_engine

    from ..database.core import get_current_revision, upgrade

    engine = create_engine(database_uri)
    redacted_url = engine.url._replace(password="[redacted]")
    current_revision = get_current_revision(engine)
    if current_revision is None:
        # Create tables and stamp (alembic) revision.
        typer.echo(
            f"Database {redacted_url} has not been initialized. Use `tiled admin initialize-database`."
        )
        raise typer.Abort()
    upgrade(engine, revision or "head")


@admin_app.command("downgrade-database")
def downgrade_database(
    database_uri: str,
    revision: str = typer.Argument(..., help="The ID of a revision to downgrade to."),
):
    """
    Upgrade the database schema to the latest version.
    """
    from sqlalchemy import create_engine

    from ..database.core import downgrade, get_current_revision

    engine = create_engine(database_uri)
    redacted_url = engine.url._replace(password="[redacted]")
    current_revision = get_current_revision(engine)
    if current_revision is None:
        # Create tables and stamp (alembic) revision.
        typer.echo(
            f"Database {redacted_url} has not been initialized. Use `tiled admin initialize-database`."
        )
        raise typer.Abort()
    downgrade(engine, revision)


@cli_app.command("login")
def login(
    uri_or_profile: str = typer.Argument(
        ..., help="URI 'http[s]://...' or a profile name."
    ),
    no_verify: bool = typer.Option(False, "--no-verify", help="Skip SSL verification."),
    show_secret_tokens: bool = typer.Option(
        False, "--show-secret-tokens", help="Show secret tokens after successful login."
    ),
    refresh_or_fail: bool = typer.Option(
        False, "--refresh-or-fail", help="Refresh authentication or exit with failure."
    ),
):
    """
    Log in to an authenticated Tiled server.
    """
    from tiled.client.utils import ClientError

    options = {}
    if refresh_or_fail:
        options["prompt_for_reauthentication"] = "never"
    try:
        client = _client_from_uri_or_profile(
            uri_or_profile, no_verify=no_verify, **options
        )
    except (ValueError, ClientError) as err:
        (msg,) = err.args
        typer.echo(msg)
        raise typer.Abort()
    if show_secret_tokens:
        from pprint import pformat

        typer.echo(pformat(dict(client.context.tokens)))


@cli_app.command("sessions")
def sessions(
    show_secret_tokens: bool = typer.Option(
        False, "--show-secret-tokens", help="Show secret (refresh) tokens."
    ),
):
    """
    List all authenticated Tiled sessions.
    """
    from tiled.client.context import sessions

    if show_secret_tokens:
        sessions_ = sessions()
        max_netloc_len = max(len(netloc) for netloc in sessions_)
        for netloc, token in sessions_.items():
            padding = max_netloc_len - len(netloc)
            typer.echo(f"{netloc}{' ' * padding}   {token}")
    else:
        for netloc in sessions():
            typer.echo(netloc)


@api_key_app.command("create")
def create_api_key(
    uri_or_profile: str = typer.Argument(
        ..., help="URI 'http[s]://...' or a profile name."
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
    client = _client_from_uri_or_profile(uri_or_profile, no_verify=no_verify)
    if not scopes:
        # This is how typer interprets unspecified scopes.
        # Replace with None to get default scopes.
        scopes = None
    info = client.context.create_api_key(
        scopes=scopes, expires_in=expires_in, note=note
    )
    # TODO Print other info to the stderr?
    typer.echo(info["secret"])


@api_key_app.command("list")
def list_api_keys(
    uri_or_profile: str = typer.Argument(
        ..., help="URI 'http[s]://...' or a profile name."
    ),
    no_verify: bool = typer.Option(False, "--no-verify", help="Skip SSL verification."),
):
    client = _client_from_uri_or_profile(uri_or_profile, no_verify=no_verify)
    info = client.context.whoami()
    if not info["api_keys"]:
        typer.echo("No API keys found")
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
    uri_or_profile: str = typer.Argument(
        ..., help="URI 'http[s]://...' or a profile name."
    ),
    first_eight: str = typer.Argument(
        ..., help="First eight characters of API key (or the whole key)"
    ),
    no_verify: bool = typer.Option(False, "--no-verify", help="Skip SSL verification."),
):
    client = _client_from_uri_or_profile(uri_or_profile, no_verify=no_verify)
    client.context.revoke_api_key(first_eight[:8])


@cli_app.command("logout")
def logout(
    uri_or_profile: Optional[str] = typer.Argument(
        None, help="URI 'http[s]://...' or a profile name. If blank, log out of all."
    ),
):
    """
    Log out from one or all authenticated Tiled servers.
    """
    from tiled.client.context import logout, logout_all

    logged_out_from = []
    if uri_or_profile is None:
        logged_out_from.extend(logout_all())
    else:
        logged_out_from.append(logout(uri_or_profile))
    for netloc in logged_out_from:
        typer.echo(netloc)


@cli_app.command("tree")
def tree(
    uri_or_profile: str = typer.Argument(
        ..., help="URI 'http[s]://...' or a profile name."
    ),
    max_lines: int = typer.Argument(20, help="Max lines to show."),
    no_verify: bool = typer.Option(False, "--no-verify", help="Skip SSL verification."),
):
    """
    Show the names of entries in a Tree.

    This is similar to the UNIX utility `tree` for listing nested directories.
    """
    from ..utils import gen_tree

    tree_obj = _client_from_uri_or_profile(uri_or_profile, no_verify=no_verify)
    for counter, line in enumerate(gen_tree(tree_obj), start=1):
        if (max_lines is not None) and (counter > max_lines):
            print(
                f"Output truncated at {max_lines} lines. "
                "Use `tiled tree CATALOG <N>` to see <N> lines."
            )
            break
        print(line)


@cli_app.command("download")
def download(
    uri_or_profile: str = typer.Argument(
        ..., help="URI 'http[s]://...' or a profile name."
    ),
    cache_path: str = typer.Argument(..., help="Local directory for cache storage"),
    available_bytes: Optional[int] = None,
    no_verify: bool = typer.Option(False, "--no-verify", help="Skip SSL verification."),
):
    """
    Download content from a Tree to an on-disk cache.
    """
    from ..client.cache import Cache, download

    cache = Cache.on_disk(cache_path, available_bytes=available_bytes)
    client = _client_from_uri_or_profile(
        uri_or_profile, cache=cache, no_verify=no_verify
    )
    download(client)


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
            "Use tiled profile list to see profile names."
        )
        raise typer.Abort()
    print(f"Source: {filepath}", file=sys.stderr)
    print("--", file=sys.stderr)
    print(yaml.dump(content), file=sys.stdout)


@serve_app.command("directory")
def serve_directory(
    directory: str,
    public: bool = typer.Option(False, "--public"),
    keep_ext: bool = typer.Option(
        False,
        "--keep-ext",
        help=(
            "Serve a file like 'measurements.csv' as its full filepath with extension, "
            "instead of the default which would serve it as 'measurements'. "
            "This is discouraged because it leaks details about the storage "
            "format to the client, such that changing the storage in the future "
            "may break user (client-side) code."
        ),
    ),
    poll_interval: float = typer.Option(
        None,
        "--poll-interval",
        help=(
            "Time in seconds between scans of the directory for removed or "
            "changed files. If 0, do not poll for changes."
        ),
    ),
    host: str = typer.Option(
        "127.0.0.1",
        help=(
            "Bind socket to this host. Use `--host 0.0.0.0` to make the application "
            "available on your local network. IPv6 addresses are supported, for "
            "example: --host `'::'`."
        ),
    ),
    port: int = typer.Option(8000, help="Bind to a socket with this port."),
    object_cache_available_bytes: Optional[float] = typer.Option(
        None,
        "--data-cache",
        help=(
            "Maximum size for the object cache, given as a number of bytes as in "
            "1_000_000 or as a fraction of system RAM (total physical memory) as in "
            "0.3. Set to 0 to disable this cache. By default, it will use up to "
            "0.15 (15%) of RAM."
        ),
    ),
):
    "Serve a Tree instance from a directory of files."
    from ..adapters.files import DirectoryAdapter
    from ..server.app import build_app, print_admin_api_key_if_generated

    tree_kwargs = {}
    server_settings = {}
    if keep_ext:
        from ..adapters.files import identity

        tree_kwargs.update({"key_from_filename": identity})
    if poll_interval is not None:
        tree_kwargs.update({"poll_interval": poll_interval})
    if object_cache_available_bytes is not None:
        server_settings["object_cache"] = {}
        server_settings["object_cache"][
            "available_bytes"
        ] = object_cache_available_bytes
    tree = DirectoryAdapter.from_directory(directory, **tree_kwargs)
    web_app = build_app(tree, {"allow_anonymous_access": public}, server_settings)
    print_admin_api_key_if_generated(web_app, host=host, port=port)

    import uvicorn

    uvicorn.run(web_app, host=host, port=port)


@serve_app.command("pyobject")
def serve_pyobject(
    object_path: str = typer.Argument(
        ..., help="Object path, as in 'package.subpackage.module:object_name'"
    ),
    public: bool = typer.Option(False, "--public"),
    host: str = typer.Option(
        "127.0.0.1",
        help=(
            "Bind socket to this host. Use `--host 0.0.0.0` to make the application "
            "available on your local network. IPv6 addresses are supported, for "
            "example: --host `'::'`."
        ),
    ),
    port: int = typer.Option(8000, help="Bind to a socket with this port."),
    object_cache_available_bytes: Optional[float] = typer.Option(
        None,
        "--data-cache",
        help=(
            "Maximum size for the object cache, given as a number of bytes as in "
            "1_000_000 or as a fraction of system RAM (total physical memory) as in "
            "0.3. Set to 0 to disable this cache. By default, it will use up to "
            "0.15 (15%) of RAM."
        ),
    ),
):
    "Serve a Tree instance from a Python module."
    from ..server.app import build_app, print_admin_api_key_if_generated
    from ..utils import import_object

    tree = import_object(object_path)
    server_settings = {}
    if object_cache_available_bytes is not None:
        server_settings["object_cache"] = {}
        server_settings["object_cache"][
            "available_bytes"
        ] = object_cache_available_bytes
    web_app = build_app(tree, {"allow_anonymous_access": public}, server_settings)
    print_admin_api_key_if_generated(web_app, host=host, port=port)

    import uvicorn

    uvicorn.run(web_app, host=host, port=port)


@serve_app.command("config")
def serve_config(
    config_path: Path = typer.Argument(
        None,
        help=(
            "Path to a config file or directory of config files. "
            "If None, check environment variable TILED_CONFIG. "
            "If that is unset, try default location ./config.yml."
        ),
    ),
    public: bool = typer.Option(False, "--public"),
    host: str = typer.Option(
        None,
        help=(
            "Bind socket to this host. Use `--host 0.0.0.0` to make the application "
            "available on your local network. IPv6 addresses are supported, for "
            "example: --host `'::'`. Uses value in config by default."
        ),
    ),
    port: int = typer.Option(
        None, help="Bind to a socket with this port. Uses value in config by default."
    ),
):
    "Serve a Tree as specified in configuration file(s)."
    import os

    from ..config import construct_build_app_kwargs, parse_configs

    config_path = config_path or os.getenv("TILED_CONFIG", "config.yml")
    try:
        parsed_config = parse_configs(config_path)
    except Exception as err:
        typer.echo(str(err))
        raise typer.Abort()

    # Let --public flag override config.
    if public:
        if "authentication" not in parsed_config:
            parsed_config["authentication"] = {}
        parsed_config["authentication"]["allow_anonymous_access"] = True

    # Delay this import so that we can fail faster if config-parsing fails above.

    from ..server.app import build_app, logger, print_admin_api_key_if_generated

    # Extract config for uvicorn.
    uvicorn_kwargs = parsed_config.pop("uvicorn", {})
    # If --host is given, it overrides host in config. Same for --port.
    uvicorn_kwargs["host"] = host or uvicorn_kwargs.get("host", "127.0.0.1")
    uvicorn_kwargs["port"] = port or uvicorn_kwargs.get("port", 8000)

    # This config was already validated when it was parsed. Do not re-validate.
    kwargs = construct_build_app_kwargs(parsed_config, source_filepath=config_path)
    logger.info(f"Using configuration from {Path(config_path).absolute()}")
    web_app = build_app(**kwargs)
    print_admin_api_key_if_generated(
        web_app, host=uvicorn_kwargs["host"], port=uvicorn_kwargs["port"]
    )

    # Likewise, delay this import.

    import uvicorn

    uvicorn.run(web_app, **uvicorn_kwargs)


def _client_from_uri_or_profile(
    uri_or_profile, no_verify, cache=None, prompt_for_reauthentication=None
):
    from ..client import from_profile, from_uri

    options = {}
    if no_verify:
        options["verify"] = False
    if prompt_for_reauthentication is not None:
        options["prompt_for_reauthentication"] = prompt_for_reauthentication
    if uri_or_profile.startswith("http://") or uri_or_profile.startswith("https://"):
        # This looks like a URI.
        uri = uri_or_profile
        return from_uri(uri, cache=cache, **options)
    else:
        from ..profiles import list_profiles

        # Is this a profile name?
        if uri_or_profile in list_profiles():
            profile_name = uri_or_profile
            return from_profile(profile_name, cache=cache, **options)
        typer.echo(
            f"Not sure what to do with tree {uri_or_profile!r}. "
            "It does not look like a URI (it does not start with http[s]://) "
            "and it does not match any profiles. Use `tiled profiles list` to "
            "see profiles."
        )
        raise typer.Abort()


main = cli_app

if __name__ == "__main__":
    main()

# This object is used by the auto-generated documentation.
typer_click_object = typer.main.get_command(cli_app)
