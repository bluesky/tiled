import re
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
    public: bool = typer.Option(
        False,
        "--public",
        help=(
            "Turns off requirement for API key authentication for reading. "
            "However, the API key is still required for writing, so data cannot be modified even with "
            "this option selected."
        ),
    ),
    api_key: str = typer.Option(
        None,
        "--api-key",
        help=(
            "Set the single-user API key. "
            "By default, a random key is generated at startup and printed."
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
    ext: List[str] = typer.Option(
        None,
        "--ext",
        help=(
            "Support custom file extension, mapping it to a known mimetype. "
            "Spell like '.tif=image/tiff'. Include the leading '.' in the file "
            "extension."
        ),
    ),
    mimetype_detection_hook: str = typer.Option(
        None,
        "--mimetype-hook",
        help=(
            "ADVANCED: Custom mimetype detection Python function. "
            "Expected interface: detect_mimetype(filepath, mimetype) -> mimetype "
            "Specify here as 'package.module:function'"
        ),
    ),
    adapters: List[str] = typer.Option(
        None,
        "--adapter",
        help=(
            "ADVANCED: Custom Tiled Adapter for reading a given format"
            "Specify here as 'mimetype=package.module:function'"
        ),
    ),
    walkers: List[str] = typer.Option(
        None,
        "--walker",
        help=(
            "ADVANCED: Custom Tiled Walker for traversing directories and "
            "grouping files. This is used in conjunction with Adapters that operate "
            "on groups of files. "
            "Specify here as 'package.module:function'"
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

    server_settings = {}
    if keep_ext:
        from ..adapters.files import identity

        key_from_filename = identity
    else:
        key_from_filename = None
    if object_cache_available_bytes is not None:
        server_settings["object_cache"] = {}
        server_settings["object_cache"][
            "available_bytes"
        ] = object_cache_available_bytes

    from logging import StreamHandler

    from ..catalog.register import logger as register_logger
    from ..catalog.register import register
    from ..catalog.register import watch as watch_

    mimetypes_by_file_ext = {}
    EXT_PATTERN = re.compile(r"(.*) *= *(.*)")
    for item in ext or []:
        match = EXT_PATTERN.match(item)
        if match is None:
            raise ValueError(
                f"Failed parsing --ext option {item}, expected format '.ext=mimetype'"
            )
        ext, mimetype = match.groups()
        mimetypes_by_file_ext[ext] = mimetype
    adapters_by_mimetype = {}
    ADAPTER_PATTERN = re.compile(r"(.*) *= *(.*)")
    for item in adapters or []:
        match = ADAPTER_PATTERN.match(item)
        if match is None:
            raise ValueError(
                f"Failed parsing --adapter option {item}, expected format 'mimetype=package.module:obj'"
            )
        mimetype, obj_ref = match.groups()
        adapters_by_mimetype[mimetype] = obj_ref
    catalog_adapter = from_uri(
        database,
        readable_storage=[directory],
        adapters_by_mimetype=adapters_by_mimetype,
    )
    typer.echo(f"Indexing '{directory}' ...")
    if verbose:
        register_logger.addHandler(StreamHandler())
        register_logger.setLevel("INFO")
    web_app = build_app(
        catalog_adapter,
        {
            "allow_anonymous_access": public,
            "single_user_api_key": api_key,
        },
        server_settings,
    )
    if watch:

        async def walk_and_serve():
            import anyio

            event = anyio.Event()
            asyncio.create_task(
                watch_(
                    catalog_adapter,
                    directory,
                    initial_walk_complete_event=event,
                    mimetype_detection_hook=mimetype_detection_hook,
                    mimetypes_by_file_ext=mimetypes_by_file_ext,
                    adapters_by_mimetype=adapters_by_mimetype,
                    walkers=walkers,
                    key_from_filename=key_from_filename,
                )
            )
            await event.wait()
            typer.echo("Initial indexing complete. Starting server...")
            print_admin_api_key_if_generated(web_app, host=host, port=port)

            import uvicorn

            config = uvicorn.Config(web_app, host=host, port=port)
            server = uvicorn.Server(config)
            await server.serve()

        asyncio.run(walk_and_serve())
    else:
        asyncio.run(
            register(
                catalog_adapter,
                directory,
                mimetype_detection_hook=mimetype_detection_hook,
                mimetypes_by_file_ext=mimetypes_by_file_ext,
                adapters_by_mimetype=adapters_by_mimetype,
                walkers=walkers,
                key_from_filename=key_from_filename,
            )
        )

        typer.echo("Indexing complete. Starting server...")
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
    api_key: str = typer.Option(
        None,
        "--api-key",
        help=(
            "Set the single-user API key. "
            "By default, a random key is generated at startup and printed."
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
    )
    web_app = build_app(
        tree,
        {
            "allow_anonymous_access": public,
            "single_user_api_key": api_key,
        },
        server_settings,
        scalable=scalable,
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
    api_key: str = typer.Option(
        None,
        "--api-key",
        help=(
            "Set the single-user API key. "
            "By default, a random key is generated at startup and printed."
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
        tree,
        {
            "allow_anonymous_access": public,
            "single_user_api_key": api_key,
        },
        server_settings,
        scalable=scalable,
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
    api_key: str = typer.Option(
        None,
        "--api-key",
        help=(
            "Set the single-user API key. "
            "By default, a random key is generated at startup and printed."
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
    # Let --api-key flag override config.
    if api_key:
        if "authentication" not in parsed_config:
            parsed_config["authentication"] = {}
        parsed_config["authentication"]["single_user_api_key"] = api_key

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
