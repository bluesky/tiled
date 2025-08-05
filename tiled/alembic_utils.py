import contextlib
import os.path
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Iterable, Optional

from alembic import command
from alembic.config import Config
from alembic.runtime import migration
from sqlalchemy.ext.asyncio import AsyncEngine


def write_alembic_ini(
    alembic_ini_template_path: Path, alembic_dir: Path, path: Path, database_uri: str
):
    """Write a complete alembic.ini from our template.

    Parameters
    ----------
    alembic_ini_template_path: str
    alembic_dir: str
    path : str
        path to the alembic.ini file that should be written.
    db_url : str
        The SQLAlchemy database url, e.g. `sqlite+aiosqlite:///tiled.sqlite`.
    """
    with open(alembic_ini_template_path) as f:
        alembic_ini_tpl = f.read()

    with open(path, "w") as f:
        f.write(
            alembic_ini_tpl.format(
                migration_script_directory=alembic_dir,
                # If there are any %s in the URL, they should be replaced with %%, since ConfigParser
                # by default uses %() for substitution. You'll get %s in your URL when you have usernames
                # with special chars (such as '@') that need to be URL encoded. URL Encoding is done with %s.
                # YAY for nested templates?
                database_uri=str(database_uri).replace("%", "%%"),
            )
        )


@contextlib.contextmanager
def temp_alembic_ini(
    alembic_ini_template_path: Path, alembic_dir: Path, database_uri: str
) -> Generator[str]:
    """
    Context manager for temporary alembic configuration file

    Temporarily write an alembic.ini file for use with alembic migration scripts.
    Context manager yields alembic.ini path.

    Parameters
    ----------
    alembic_ini_template_path: str
    alembic_dir: str
    database_uri : str
        The SQLAlchemy database url, e.g. `sqlite+aiosqlite:///tiled.sqlite`.

    Returns
    -------
    alembic_ini: str
        The path to the temporary alembic.ini that we have created.
        This file will be cleaned up on exit from the context manager.
    """
    with tempfile.TemporaryDirectory() as td:
        alembic_ini = os.path.join(td, "alembic.ini")
        write_alembic_ini(
            alembic_ini_template_path, alembic_dir, alembic_ini, database_uri
        )
        yield alembic_ini


def stamp_head(alembic_ini_template_path: Path, alembic_dir: Path, engine_url: str):
    """
    Upgrade schema to the specified revision.
    """
    with temp_alembic_ini(
        alembic_ini_template_path, alembic_dir, engine_url
    ) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        command.stamp(alembic_cfg, "head")


def upgrade(
    alembic_ini_template_path: Path, alembic_dir: Path, engine_url: str, revision: str
):
    """
    Upgrade schema to the specified revision.
    """
    with temp_alembic_ini(
        alembic_ini_template_path, alembic_dir, engine_url
    ) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        command.upgrade(alembic_cfg, revision)


def downgrade(
    alembic_ini_template_path: Path, alembic_dir: Path, engine_url: str, revision: str
):
    """
    Downgrade schema to the specified revision.
    """
    with temp_alembic_ini(
        alembic_ini_template_path, alembic_dir, engine_url
    ) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        command.downgrade(alembic_cfg, revision)


class UnrecognizedDatabase(Exception):
    pass


class UninitializedDatabase(Exception):
    pass


class DatabaseUpgradeNeeded(Exception):
    pass


async def get_current_revision(
    engine: AsyncEngine, known_revisions: Iterable[str]
) -> Optional[str]:
    redacted_url = engine.url._replace(password="[redacted]")
    async with engine.connect() as conn:
        context = await conn.run_sync(migration.MigrationContext.configure)
        heads = await conn.run_sync(lambda conn: context.get_current_heads())
    if heads == ():
        return None
    elif len(heads) != 1:
        raise UnrecognizedDatabase(
            f"This database {redacted_url} is stamped with an alembic revisions {heads}. "
            "It looks like Tiled has been configured to connect to a database "
            "already populated by some other application (not Tiled) or else "
            "its database is in a corrupted state."
        )
    (revision,) = heads
    if revision not in known_revisions:
        raise UnrecognizedDatabase(
            f"The database {redacted_url} has an unrecognized revision {revision}. "
            "It may have been created by a newer version of Tiled."
        )
    return revision


async def check_database(
    engine: AsyncEngine, required_revision: str, known_revisions: Iterable[str]
):
    revision = await get_current_revision(engine, known_revisions)
    redacted_url = engine.url._replace(password="[redacted]")
    if revision is None:
        raise UninitializedDatabase(
            f"The database {redacted_url} has no revision stamp. It may be empty. "
            "It can be initialized with `initialize_database(engine)`."
        )
    elif revision != required_revision:
        raise DatabaseUpgradeNeeded(
            f"The database {redacted_url} has revision {revision} and "
            f"needs to be upgraded to revision {required_revision}."
        )
