import os
import re
from pathlib import Path
from typing import List, Optional

import typer

serve_app = typer.Typer(no_args_is_help=True)

SQLITE_CATALOG_FILENAME = "catalog.db"
DUCKDB_TABULAR_DATA_FILENAME = "data.duckdb"
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
    api_key: Optional[str] = typer.Option(
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
    ext: Optional[List[str]] = typer.Option(
        None,
        "--ext",
        help=(
            "Support custom file extension, mapping it to a known mimetype. "
            "Spell like '.tif=image/tiff'. Include the leading '.' in the file "
            "extension."
        ),
    ),
    mimetype_detection_hook: Optional[str] = typer.Option(
        None,
        "--mimetype-hook",
        help=(
            "ADVANCED: Custom mimetype detection Python function. "
            "Expected interface: detect_mimetype(filepath, mimetype) -> mimetype "
            "Specify here as 'package.module:function'"
        ),
    ),
    adapters: Optional[List[str]] = typer.Option(
        None,
        "--adapter",
        help=(
            "ADVANCED: Custom Tiled Adapter for reading a given format"
            "Specify here as 'mimetype=package.module:function'"
        ),
    ),
    walkers: Optional[List[str]] = typer.Option(
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
    log_config: Optional[str] = typer.Option(
        None, help="Custom uvicorn logging configuration file"
    ),
    log_timestamps: bool = typer.Option(
        False, help="Include timestamps in log output."
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
    database = f"sqlite:///{Path(temp_directory, SQLITE_CATALOG_FILENAME)}"

    # Because this is a tempfile we know this is a fresh database and we do not
    # need to check its current state.
    # We _will_ go ahead and stamp it with a revision because it is possible the
    # user will copy it into a permanent location.

    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import stamp_head
    from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
    from ..catalog.core import initialize_database
    from ..utils import ensure_specified_sql_driver

    database = ensure_specified_sql_driver(database)
    engine = create_async_engine(database)
    asyncio.run(initialize_database(engine))
    stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database)

    from ..catalog import from_uri as catalog_from_uri
    from ..server.app import build_app, print_server_info

    if keep_ext:
        from ..adapters.files import identity

        key_from_filename = identity
    else:
        key_from_filename = None

    from logging import StreamHandler

    from ..client.register import logger as register_logger
    from ..client.register import register
    from ..client.register import watch as watch_

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
    catalog_adapter = catalog_from_uri(
        ensure_specified_sql_driver(database),
        readable_storage=[directory],
        adapters_by_mimetype=adapters_by_mimetype,
    )
    if verbose:
        from tiled.catalog.adapter import logger as catalog_logger

        catalog_logger.addHandler(StreamHandler())
        catalog_logger.setLevel("INFO")
        register_logger.addHandler(StreamHandler())
        register_logger.setLevel("INFO")
    # Set the API key manually here, rather than letting the server do it,
    # so that we can pass it to the client.
    generated = False
    if api_key is None:
        api_key = os.getenv("TILED_SINGLE_USER_API_KEY")
        if api_key is None:
            # Lazily import server settings here to avoid server dependencies
            # in client-only environments.
            from tiled.server.settings import get_settings

            api_key = get_settings().single_user_api_key
            generated = True

    web_app = build_app(
        catalog_adapter,
        {
            "allow_anonymous_access": public,
            "single_user_api_key": api_key,
        },
    )
    import functools

    import anyio
    import uvicorn

    from ..client import from_uri as client_from_uri

    print_server_info(web_app, host=host, port=port, include_api_key=generated)
    log_config = _setup_log_config(log_config, log_timestamps)
    config = uvicorn.Config(web_app, host=host, port=port, log_config=log_config)
    server = uvicorn.Server(config)

    async def run_server():
        await server.serve()

    async def wait_for_server():
        "Wait for server to start up, or raise TimeoutError."
        for _ in range(100):
            await asyncio.sleep(0.1)
            if server.started:
                break
        else:
            raise TimeoutError("Server did not start in 10 seconds.")
        host, port = server.servers[0].sockets[0].getsockname()
        api_url = f"http://{host}:{port}/api/v1/"
        return api_url

    if watch:

        async def serve_and_walk():
            server_task = asyncio.create_task(run_server())
            api_url = await wait_for_server()
            # When we add an AsyncClient for Tiled, use that here.
            client = await anyio.to_thread.run_sync(
                functools.partial(client_from_uri, api_url, api_key=api_key)
            )

            typer.echo(f"Server is up. Indexing files in {directory}...")
            event = anyio.Event()
            asyncio.create_task(
                watch_(
                    client,
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
            typer.echo("Initial indexing complete. Watching for changes...")
            await server_task

    else:

        async def serve_and_walk():
            server_task = asyncio.create_task(run_server())
            api_url = await wait_for_server()
            # When we add an AsyncClient for Tiled, use that here.
            client = await anyio.to_thread.run_sync(
                functools.partial(client_from_uri, api_url, api_key=api_key)
            )

            typer.echo(f"Server is up. Indexing files in {directory}...")
            await register(
                client,
                directory,
                mimetype_detection_hook=mimetype_detection_hook,
                mimetypes_by_file_ext=mimetypes_by_file_ext,
                adapters_by_mimetype=adapters_by_mimetype,
                walkers=walkers,
                key_from_filename=key_from_filename,
            )
            typer.echo("Indexing complete.")
            await server_task

    asyncio.run(serve_and_walk())


def serve_catalog(
    database: Optional[str] = typer.Argument(
        None, help="A filepath or database URI, e.g. 'catalog.db'"
    ),
    read: Optional[List[str]] = typer.Option(
        None,
        "--read",
        "-r",
        help="Locations that the server may read from",
    ),
    write: Optional[List[str]] = typer.Option(
        None,
        "--write",
        "-w",
        help="Locations that the server may write to",
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
    api_key: Optional[str] = typer.Option(
        None,
        "--api-key",
        help=(
            "Set the single-user API key. "
            "By default, a random key is generated at startup and printed."
        ),
    ),
    cache_uri: Optional[str] = typer.Option(
        None, "--cache", help=("Provide cache URI")
    ),
    cache_ttl: Optional[int] = typer.Option(
        None, "--cache-ttl", help=("Provide cache ttl")
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
    scalable: bool = typer.Option(
        False,
        "--scalable",
        help=(
            "This verifies that the configuration is compatible with scaled (multi-process) deployments."
        ),
    ),
    log_config: Optional[str] = typer.Option(
        None, help="Custom uvicorn logging configuration file"
    ),
    log_timestamps: bool = typer.Option(
        False, help="Include timestamps in log output."
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help=("Log details of catalog creation."),
    ),
):
    "Serve a catalog."
    import urllib.parse

    from ..catalog import from_uri
    from ..server.app import build_app, print_server_info

    parsed_database = urllib.parse.urlparse(database)
    if parsed_database.scheme in ("", "file"):
        database = f"sqlite:///{parsed_database.path}"

    write = write or []
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
        typer.echo(
            f"Initializing temporary storage in {directory}",
            err=True,
        )
        directory.mkdir()
        database = f"sqlite:///{Path(directory, SQLITE_CATALOG_FILENAME)}"

        # Because this is a tempfile we know this is a fresh database and we do not
        # need to check its current state.
        # We _will_ go ahead and stamp it with a revision because it is possible the
        # user will copy it into a permanent location.

        import asyncio

        from sqlalchemy.ext.asyncio import create_async_engine

        from ..alembic_utils import stamp_head
        from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
        from ..catalog.core import initialize_database
        from ..utils import ensure_specified_sql_driver

        database = ensure_specified_sql_driver(database)
        typer.echo(
            f"  catalog database:          {directory / SQLITE_CATALOG_FILENAME}",
            err=True,
        )
        engine = create_async_engine(database)
        asyncio.run(initialize_database(engine))
        stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database)

        if not write:
            typer.echo(
                f"  writable file storage:     {directory / DATA_SUBDIRECTORY}",
                err=True,
            )
            writable_dir = directory / DATA_SUBDIRECTORY
            writable_dir.mkdir()
            write.append(writable_dir)
            typer.echo(
                f"  writable tabular storage:  {directory / DUCKDB_TABULAR_DATA_FILENAME}",
                err=True,
            )
            tabular_data_database = (
                f"duckdb:///{Path(directory, DUCKDB_TABULAR_DATA_FILENAME)}"
            )
            write.append(tabular_data_database)
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
    elif verbose:
        from logging import StreamHandler

        from tiled.catalog.adapter import logger as catalog_logger

        catalog_logger.addHandler(StreamHandler())
        catalog_logger.setLevel("INFO")

    if not write:
        typer.echo(
            "This catalog will be served as read-only. "
            "To make it writable, specify a writable directory with --write.",
            err=True,
        )

    cache_settings = {}
    if cache_uri:
        cache_settings["uri"] = cache_uri
    if cache_ttl:
        cache_settings["ttl"] = cache_ttl

    tree = from_uri(
        database,
        writable_storage=write,
        readable_storage=read,
        init_if_not_exists=init,
        cache_settings=cache_settings,
    )
    web_app = build_app(
        tree,
        {
            "allow_anonymous_access": public,
            "single_user_api_key": api_key,
        },
        scalable=scalable,
    )
    print_server_info(web_app, host=host, port=port, include_api_key=api_key is None)

    import uvicorn

    log_config = _setup_log_config(log_config, log_timestamps)
    uvicorn.run(web_app, host=host, port=port, log_config=log_config)


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
    api_key: Optional[str] = typer.Option(
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
    scalable: bool = typer.Option(
        False,
        "--scalable",
        help=(
            "This verifies that the configuration is compatible with scaled (multi-process) deployments."
        ),
    ),
    log_config: Optional[str] = typer.Option(
        None, help="Custom uvicorn logging configuration file"
    ),
    log_timestamps: bool = typer.Option(
        False, help="Include timestamps in log output."
    ),
):
    "Serve a Tree instance from a Python module."
    from ..server.app import build_app, print_server_info
    from ..utils import import_object

    tree = import_object(object_path)
    web_app = build_app(
        tree,
        {
            "allow_anonymous_access": public,
            "single_user_api_key": api_key,
        },
        scalable=scalable,
    )
    print_server_info(web_app, host=host, port=port, include_api_key=api_key is None)

    import uvicorn

    log_config = _setup_log_config(log_config, log_timestamps)
    uvicorn.run(web_app, host=host, port=port, log_config=log_config)


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
    from ..server.app import build_app, print_server_info
    from ..utils import import_object

    EXAMPLE = "tiled.examples.generated:tree"
    tree = import_object(EXAMPLE)
    web_app = build_app(tree, {"allow_anonymous_access": True}, {})
    print_server_info(web_app, host=host, port=port, include_api_key=True)

    import uvicorn

    uvicorn.run(web_app, host=host, port=port)


@serve_app.command("config")
def serve_config(
    config_path: Optional[Path] = typer.Argument(
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
    api_key: Optional[str] = typer.Option(
        None,
        "--api-key",
        help=(
            "Set the single-user API key. "
            "By default, a random key is generated at startup and printed."
        ),
    ),
    host: Optional[str] = typer.Option(
        None,
        help=(
            "Bind socket to this host. Use `--host 0.0.0.0` to make the application "
            "available on your local network. IPv6 addresses are supported, for "
            "example: --host `'::'`. Uses value in config by default."
        ),
    ),
    port: Optional[int] = typer.Option(
        None, help="Bind to a socket with this port. Uses value in config by default."
    ),
    scalable: bool = typer.Option(
        False,
        "--scalable",
        help=(
            "This verifies that the configuration is compatible with scaled (multi-process) deployments."
        ),
    ),
    log_config: Optional[str] = typer.Option(
        None, help="Custom uvicorn logging configuration file"
    ),
    log_timestamps: bool = typer.Option(
        False, help="Include timestamps in log output."
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

    from ..server.app import build_app_from_config, logger, print_server_info

    # Extract config for uvicorn.
    uvicorn_kwargs = parsed_config.pop("uvicorn", {})
    # If --host is given, it overrides host in config. Same for --port and --log-config.
    uvicorn_kwargs["host"] = host or uvicorn_kwargs.get("host", "127.0.0.1")
    if port is None:
        port = uvicorn_kwargs.get("port", 8000)
    uvicorn_kwargs["port"] = port
    uvicorn_kwargs["log_config"] = _setup_log_config(
        log_config or uvicorn_kwargs.get("log_config"),
        log_timestamps,
    )

    # This config was already validated when it was parsed. Do not re-validate.
    logger.info(f"Using configuration from {Path(config_path).absolute()}")

    if root_path := uvicorn_kwargs.get("root_path", ""):
        parsed_config["root_path"] = root_path

    web_app = build_app_from_config(
        parsed_config, source_filepath=config_path, scalable=scalable
    )
    print_server_info(
        web_app,
        host=uvicorn_kwargs["host"],
        port=uvicorn_kwargs["port"],
        include_api_key=api_key is None,
    )

    # Likewise, delay this import.

    import uvicorn

    uvicorn.run(web_app, **uvicorn_kwargs)


def _setup_log_config(log_config, log_timestamps):
    if log_config is None:
        from ..server.logging_config import LOGGING_CONFIG

        log_config = LOGGING_CONFIG

    if log_timestamps:
        import copy

        log_config = copy.deepcopy(log_config)
        try:
            log_config["formatters"]["access"]["format"] = (
                "[%(asctime)s.%(msecs)03dZ] "
                + log_config["formatters"]["access"]["format"]
            )
            log_config["formatters"]["default"]["format"] = (
                "[%(asctime)s.%(msecs)03dZ] "
                + log_config["formatters"]["default"]["format"]
            )
        except KeyError:
            typer.echo(
                "The --log-timestamps option is only applicable with a logging "
                "configuration that, like the default logging configuration, has "
                "formatters 'access' and 'default'."
            )
            raise typer.Abort()
    return log_config
