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

    from ..catalog.adapter import initialize_database

    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme in ("", "file"):
        uri = f"sqlite+aiosqlite:///{parsed.path}"
    engine = create_async_engine(uri)
    asyncio.run(initialize_database(engine))
