import asyncio
from pathlib import Path

import typer

from ._serve import serve_catalog

catalog_app = typer.Typer()
# Support both `tiled serve catalog` and `tiled catalog serve` as synonyms
# because I cannot decide which is right.
catalog_app.command("serve")(serve_catalog)
DEFAULT_SQLITE_CATALOG_FILENAME = "catalog.db"


@catalog_app.command("init")
def init(
    uri: str = typer.Argument(
        Path.cwd() / DEFAULT_SQLITE_CATALOG_FILENAME, help="A filepath or database URI"
    )
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
    import urllib.parse

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import UninitializedDatabase, check_database, stamp_head
    from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
    from ..catalog.core import ALL_REVISIONS, REQUIRED_REVISION, initialize_database

    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme in ("", "file"):
        uri = f"sqlite+aiosqlite:///{parsed.path}"
    engine = create_async_engine(uri)

    async def do_setup():
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
            typer.echo(f"Database at {redacted_url} is already initialized.", err=True)
            raise typer.Abort()
        await engine.dispose()

    asyncio.run(do_setup())
    stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, uri)
