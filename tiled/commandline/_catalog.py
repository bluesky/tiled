import asyncio
import re
from pathlib import Path
from typing import List, Optional

import typer

from ._serve import serve_catalog

catalog_app = typer.Typer()
# Support both `tiled serve catalog` and `tiled catalog serve` as synonyms
# because I cannot decide which is right.
catalog_app.command("serve")(serve_catalog)
DEFAULT_SQLITE_CATALOG_FILENAME = "catalog.db"


@catalog_app.command("init")
def init(
    database: str = typer.Argument(
        Path.cwd() / DEFAULT_SQLITE_CATALOG_FILENAME, help="A filepath or database URI"
    ),
    if_not_exists: bool = typer.Option(
        False,
        help=(
            "By default, it is an error if a database is already initialized."
            "Set this flag to be permissive and return without an error."
        ),
    ),
):
    """
    Initialize a database as a Tiled Catalog.

    Examples:

    # Using a simple local file as an embedded "database" (SQLite)
    tiled init catalog.db
    tiled init path/to/catalog.db
    tiled init sqlite+aiosqlite:////path/to/catalog.db

    # Using a client/serve database engine (PostgreSQL)
    tiled init postgresql+asyncpg://uesrname:password@localhost/database_name:5432
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import UninitializedDatabase, check_database, stamp_head
    from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
    from ..catalog.core import ALL_REVISIONS, REQUIRED_REVISION, initialize_database
    from ..catalog.utils import SCHEME_PATTERN

    if not SCHEME_PATTERN.match(database):
        # Interpret URI as filepath.
        database = f"sqlite+aiosqlite:///{database}"

    async def do_setup():
        engine = create_async_engine(database)
        redacted_url = engine.url._replace(password="[redacted]")
        try:
            await check_database(engine, REQUIRED_REVISION, ALL_REVISIONS)
        except UninitializedDatabase:
            # Create tables and stamp (alembic) revision.
            typer.echo(
                f"Database {redacted_url} is new. Creating tables.",
                err=True,
            )
            await initialize_database(engine)
            typer.echo("Database initialized.", err=True)
        else:
            if not if_not_exists:
                typer.echo(
                    f"Database at {redacted_url} is already initialized.", err=True
                )
                raise typer.Abort()
        finally:
            await engine.dispose()

    asyncio.run(do_setup())
    stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database)


@catalog_app.command("register")
def register(
    database: str = typer.Argument(
        ..., help="A filepath or database URI for the catalog"
    ),
    filepath: str = typer.Argument(..., help="A file or directory to register"),
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
    prefix: str = typer.Option(
        "/", help="Location within the catalog's namespace to register these files"
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
):
    from ..catalog.utils import SCHEME_PATTERN

    if not SCHEME_PATTERN.match(database):
        # Interpret URI as filepath.
        database = f"sqlite+aiosqlite:///{database}"
    if keep_ext:
        from ..catalog.register import identity

        key_from_filename = identity
    else:
        key_from_filename = None
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

    from ..catalog import from_uri

    catalog_adapter = from_uri(database)

    from logging import StreamHandler

    from ..catalog.register import logger as register_logger
    from ..catalog.register import register
    from ..catalog.register import watch as watch_

    if verbose:
        register_logger.addHandler(StreamHandler())
        register_logger.setLevel("INFO")
    if watch:
        asyncio.run(
            watch_(
                catalog_adapter,
                filepath,
                prefix=prefix,
                mimetype_detection_hook=mimetype_detection_hook,
                mimetypes_by_file_ext=mimetypes_by_file_ext,
                adapters_by_mimetype=adapters_by_mimetype,
                walkers=walkers,
                key_from_filename=key_from_filename,
            )
        )
    else:
        asyncio.run(
            register(
                catalog_adapter,
                filepath,
                prefix=prefix,
                mimetype_detection_hook=mimetype_detection_hook,
                mimetypes_by_file_ext=mimetypes_by_file_ext,
                adapters_by_mimetype=adapters_by_mimetype,
                walkers=walkers,
                key_from_filename=key_from_filename,
            )
        )


@catalog_app.command("upgrade-database")
def upgrade_database(
    database_uri: str,
    revision: Optional[str] = typer.Argument(
        None,
        help="The ID of a revision to upgrade to. By default, upgrade to the latest one.",
    ),
):
    """
    Upgrade the catalog database schema to the latest version.
    """
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import get_current_revision, upgrade
    from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
    from ..catalog.core import ALL_REVISIONS

    async def do_setup():
        engine = create_async_engine(database_uri)
        redacted_url = engine.url._replace(password="[redacted]")
        current_revision = await get_current_revision(engine, ALL_REVISIONS)
        await engine.dispose()
        if current_revision is None:
            # Create tables and stamp (alembic) revision.
            typer.echo(
                f"Database {redacted_url} has not been initialized. Use `tiled catalog init`.",
                err=True,
            )
            raise typer.Abort()

    asyncio.run(do_setup())
    upgrade(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri, revision or "head")


@catalog_app.command("downgrade-database")
def downgrade_database(
    database_uri: str,
    revision: str = typer.Argument(..., help="The ID of a revision to downgrade to."),
):
    """
    Upgrade the catalog database schema to the latest version.
    """
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import downgrade, get_current_revision
    from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
    from ..catalog.core import ALL_REVISIONS

    async def do_setup():
        engine = create_async_engine(database_uri)
        redacted_url = engine.url._replace(password="[redacted]")
        current_revision = await get_current_revision(engine, ALL_REVISIONS)
        if current_revision is None:
            # Create tables and stamp (alembic) revision.
            typer.echo(
                f"Database {redacted_url} has not been initialized. Use `tiled catalog init`.",
                err=True,
            )
            raise typer.Abort()

    asyncio.run(do_setup())
    downgrade(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri, revision)
