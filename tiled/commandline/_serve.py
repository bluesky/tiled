from pathlib import Path
from typing import List, Optional

import typer

serve_app = typer.Typer()

SQLITE_CATALOG_FILENAME = "catalog.db"
DATA_SUBDIRECTORY = "data"


@serve_app.command("directory")
def serve_directory(
    directory: str = typer.Argument(..., help="A directory to serve"),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help=("Log details of directory traversal and file registration."),
    ),
    watch: bool = typer.Option(
        False,
        "--watch",
        "-w",
        help="Update catalog when files are added, removed, or changed.",
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
):
    "Serve a Tree instance from a directory of files."
    import tempfile

    temp_directory = Path(tempfile.TemporaryDirectory().name)
    temp_directory.mkdir()
    typer.echo(
        f"Creating catalog database at {temp_directory / SQLITE_CATALOG_FILENAME}",
        err=True,
    )
    database = f"sqlite+aiosqlite:///{Path(temp_directory, SQLITE_CATALOG_FILENAME)}"

    # Because this is a tempfile we know this is a fresh database and we do not
    # need to check its current state.
    # We _will_ go ahead and stamp it with a revision because it is possible the
    # user will copy it into a permanent location.

    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import stamp_head
    from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
    from ..catalog.core import initialize_database

    engine = create_async_engine(database)
    asyncio.run(initialize_database(engine))
    stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database)

    from ..catalog import from_uri
    from ..server.app import build_app, print_admin_api_key_if_generated

    tree_kwargs = {}
    server_settings = {}
    if keep_ext:
        from ..adapters.files import identity

        tree_kwargs.update({"key_from_filename": identity})
    if object_cache_available_bytes is not None:
        server_settings["object_cache"] = {}
        server_settings["object_cache"][
            "available_bytes"
        ] = object_cache_available_bytes
    catalog_adapter = from_uri(database, readable_storage=[directory], **tree_kwargs)

    from logging import StreamHandler

    from ..catalog.register import logger as register_logger
    from ..catalog.register import register
    from ..catalog.register import watch as watch_

    typer.echo(f"Indexing '{directory}' ...")
    if verbose:
        register_logger.addHandler(StreamHandler())
        register_logger.setLevel("INFO")
    if watch:
        from multiprocessing import Event, Process

        def target(sync_mp_event):
            import anyio

            class _AsyncEventWrapper:
                def __init__(self, event):
                    self.event = event

                async def set(self):
                    return await anyio.to_thread.run_sync(self.event.set)

                def is_set(self):
                    return self.event.is_set()

            event = _AsyncEventWrapper(sync_mp_event)

            async def f(event):
                await event.set()

            asyncio.run(
                watch_(catalog_adapter, directory, initial_walk_complete_event=event)
            )
            # asyncio.run(f(event))

        sync_mp_event = Event()

        process = Process(target=target, args=(sync_mp_event,))
        process.start()
        # Block until initial walk is complete.
        print("BLOCKING")
        sync_mp_event.wait()
        print("UNBLOCKED")
    else:
        asyncio.run(register(catalog_adapter, directory))

    typer.echo("Indexing complete. Starting server...")
    web_app = build_app(
        catalog_adapter,
        {"allow_anonymous_access": public},
        server_settings,
    )
    print_admin_api_key_if_generated(web_app, host=host, port=port)

    import uvicorn

    uvicorn.run(web_app, host=host, port=port)


def serve_catalog(
    database: str = typer.Argument(
        None, help="A filepath or database URI, e.g. 'catalog.db'"
    ),
    read: List[str] = typer.Option(
        None,
        "--read",
        "-r",
        help="Locations that the server may read from",
    ),
    write: str = typer.Option(
        None,
        "--write",
        "-w",
        help="Location that the server may write to",
    ),
    temp: bool = typer.Option(
        False,
        "--temp",
        help="Make a new catalog in a temporary directory.",
    ),
    init: bool = typer.Option(
        False,
        "--init",
        help="Initialize a new catalog database.",
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
    "Serve a catalog."
    import urllib.parse

    from ..catalog import from_uri
    from ..server.app import build_app, print_admin_api_key_if_generated

    parsed_database = urllib.parse.urlparse(database)
    if parsed_database.scheme in ("", "file"):
        database = f"sqlite+aiosqlite:///{parsed_database.path}"

    if temp:
        if database is not None:
            typer.echo(
                "The option --temp was set but a database was also provided. "
                "Do one or the other.",
                err=True,
            )
            raise typer.Abort()
        import tempfile

        directory = Path(tempfile.TemporaryDirectory().name)
        directory.mkdir()
        typer.echo(
            f"Creating catalog database at {directory / SQLITE_CATALOG_FILENAME}",
            err=True,
        )
        typer.echo(
            f"Creating writable catalog data directory at {directory / DATA_SUBDIRECTORY}",
            err=True,
        )
        database = f"sqlite+aiosqlite:///{Path(directory, SQLITE_CATALOG_FILENAME)}"

        # Because this is a tempfile we know this is a fresh database and we do not
        # need to check its current state.
        # We _will_ go ahead and stamp it with a revision because it is possible the
        # user will copy it into a permanent location.

        import asyncio

        from sqlalchemy.ext.asyncio import create_async_engine

        from ..alembic_utils import stamp_head
        from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
        from ..catalog.core import initialize_database

        engine = create_async_engine(database)
        asyncio.run(initialize_database(engine))
        stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database)

        if write is None:
            write = directory / DATA_SUBDIRECTORY
            write.mkdir()
        # TODO Hook into server lifecycle hooks to delete this at shutdown.
    elif database is None:
        typer.echo(
            """A catalog must be specified. Either use a temporary catalog:

    tiled serve catalog --temp

or initialize a new catalog, e.g.

    tiled catalog init catalog.db
    tiled serve catalog catalog.db

or use an existing one:

    tiled serve catalog catalog.db
""",
            err=True,
        )
        raise typer.Abort()

    if write is None:
        typer.echo(
            "This catalog will be served as read-only. "
            "To make it writable, specify a writable directory with --write.",
            err=True,
        )
    tree_kwargs = {}
    server_settings = {}
    if object_cache_available_bytes is not None:
        server_settings["object_cache"] = {}
        server_settings["object_cache"][
            "available_bytes"
        ] = object_cache_available_bytes
    tree = from_uri(
        database,
        writable_storage=write,
        readable_storage=read,
        init_if_not_exists=init,
        **tree_kwargs,
    )
    web_app = build_app(
        tree, {"allow_anonymous_access": public}, server_settings, scalable=scalable
    )
    print_admin_api_key_if_generated(web_app, host=host, port=port)

    import uvicorn

    uvicorn.run(web_app, host=host, port=port)


serve_app.command("catalog")(serve_catalog)


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
