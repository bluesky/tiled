from pathlib import Path
from typing import Optional

import typer

serve_app = typer.Typer()


@serve_app.command("directory")
def serve_directory(
    directory: str,
    public: bool = typer.Option(
        False,
        "--public",
        help=(
            "Turns off requirement for API key authentication for reading. "
            "However, the API key is still required for writing, so data cannot be modified even with "
            "this option selected."
        ),
    ),
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
    scalable: bool = typer.Option(
        False,
        "--scalable",
        help=(
            "This verifies that the configuration is compatible with scaled (multi-process) deployments."
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
    web_app = build_app(
        tree, {"allow_anonymous_access": public}, server_settings, scalable=scalable
    )
    print_admin_api_key_if_generated(web_app, host=host, port=port)

    import uvicorn

    uvicorn.run(web_app, host=host, port=port)


@serve_app.command("writable")
def serve_writable(
    directory: str,
    init: bool = typer.Option(
        False,
        "--init",
        help="Make a new writable space and catalog (database).",
    ),
    public: bool = typer.Option(
        False,
        "--public",
        help=(
            "Turns off requirement for API key authentication for reading. "
            "However, the API key is still required for writing, so data cannot be modified even with "
            "this option selected."
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
    scalable: bool = typer.Option(
        False,
        "--scalable",
        help=(
            "This verifies that the configuration is compatible with scaled (multi-process) deployments."
        ),
    ),
):
    "Serve a writable storage area."
    from ..catalog import from_uri
    from ..server.app import build_app, print_admin_api_key_if_generated

    tree_kwargs = {}
    server_settings = {}
    if object_cache_available_bytes is not None:
        server_settings["object_cache"] = {}
        server_settings["object_cache"][
            "available_bytes"
        ] = object_cache_available_bytes
    sqlite_filename = "tiled_catalog.sqlite"
    uri = f"sqlite+aiosqlite:///{Path(directory, sqlite_filename)}"
    tree = from_uri(
        uri,
        writable_storage=directory,
        initialize_database_at_startup=init,
        **tree_kwargs,
    )
    web_app = build_app(
        tree, {"allow_anonymous_access": public}, server_settings, scalable=scalable
    )
    print_admin_api_key_if_generated(web_app, host=host, port=port)

    import uvicorn

    uvicorn.run(web_app, host=host, port=port)


@serve_app.command("pyobject")
def serve_pyobject(
    object_path: str = typer.Argument(
        ..., help="Object path, as in 'package.subpackage.module:object_name'"
    ),
    public: bool = typer.Option(
        False,
        "--public",
        help=(
            "Turns off requirement for API key authentication for reading. "
            "However, the API key is still required for writing, so data cannot be modified even with this "
            "option selected."
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
    scalable: bool = typer.Option(
        False,
        "--scalable",
        help=(
            "This verifies that the configuration is compatible with scaled (multi-process) deployments."
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
    web_app = build_app(
        tree, {"allow_anonymous_access": public}, server_settings, scalable=scalable
    )
    print_admin_api_key_if_generated(web_app, host=host, port=port)

    import uvicorn

    uvicorn.run(web_app, host=host, port=port)


@serve_app.command("demo")
def serve_demo(
    host: str = typer.Option(
        "127.0.0.1",
        help=(
            "Bind socket to this host. Use `--host 0.0.0.0` to make the application "
            "available on your local network. IPv6 addresses are supported, for "
            "example: --host `'::'`."
        ),
    ),
    port: int = typer.Option(8000, help="Bind to a socket with this port."),
):
    "Start a public server with example data."
    from ..server.app import build_app, print_admin_api_key_if_generated
    from ..utils import import_object

    EXAMPLE = "tiled.examples.generated:tree"
    tree = import_object(EXAMPLE)
    web_app = build_app(tree, {"allow_anonymous_access": True}, {})
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
    public: bool = typer.Option(
        False,
        "--public",
        help=(
            "Turns off requirement for API key authentication for reading. "
            "However, the API key is still required for writing, so data cannot be modified even with this "
            "option selected."
        ),
    ),
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
    scalable: bool = typer.Option(
        False,
        "--scalable",
        help=(
            "This verifies that the configuration is compatible with scaled (multi-process) deployments."
        ),
    ),
):
    "Serve a Tree as specified in configuration file(s)."
    import os

    from ..config import parse_configs

    config_path = config_path or os.getenv("TILED_CONFIG", "config.yml")
    try:
        parsed_config = parse_configs(config_path)
    except Exception as err:
        typer.echo(str(err), err=True)
        raise typer.Abort()

    # Let --public flag override config.
    if public:
        if "authentication" not in parsed_config:
            parsed_config["authentication"] = {}
        parsed_config["authentication"]["allow_anonymous_access"] = True

    # Delay this import so that we can fail faster if config-parsing fails above.

    from ..server.app import (
        build_app_from_config,
        logger,
        print_admin_api_key_if_generated,
    )

    # Extract config for uvicorn.
    uvicorn_kwargs = parsed_config.pop("uvicorn", {})
    # If --host is given, it overrides host in config. Same for --port.
    uvicorn_kwargs["host"] = host or uvicorn_kwargs.get("host", "127.0.0.1")
    uvicorn_kwargs["port"] = port or uvicorn_kwargs.get("port", 8000)

    # This config was already validated when it was parsed. Do not re-validate.
    logger.info(f"Using configuration from {Path(config_path).absolute()}")
    web_app = build_app_from_config(
        parsed_config, source_filepath=config_path, scalable=scalable
    )
    print_admin_api_key_if_generated(
        web_app, host=uvicorn_kwargs["host"], port=uvicorn_kwargs["port"]
    )

    # Likewise, delay this import.

    import uvicorn

    uvicorn.run(web_app, **uvicorn_kwargs)
