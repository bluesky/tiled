import asyncio
import concurrent.futures
import contextlib
import os.path
import tempfile
from collections.abc import Awaitable, Callable, Generator, Iterable
from pathlib import Path
from typing import TypeVar

from alembic import command
from alembic.config import Config
from alembic.runtime import migration
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

T = TypeVar("T")


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
) -> str | None:
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
    engine: AsyncEngine,
    required_revision: str,
    known_revisions: Iterable[str],
    accepted_revisions: Iterable[str] = (),
) -> str:
    """
    Check that the database is at `required_revision`; return its revision.

    A database at one of `accepted_revisions` passes as well (see
    `revisions_accepted_at_startup`).
    """
    revision = await get_current_revision(engine, known_revisions)
    redacted_url = engine.url._replace(password="[redacted]")
    if revision is None:
        raise UninitializedDatabase(
            f"The database {redacted_url} has no revision stamp. It may be empty. "
            "It can be initialized with `initialize_database(engine)`."
        )
    elif revision != required_revision and revision not in accepted_revisions:
        raise DatabaseUpgradeNeeded(
            f"The database {redacted_url} has revision {revision} and "
            f"needs to be upgraded to revision {required_revision}."
        )
    return revision


def _run_outside_event_loop(func: Callable[[], T]) -> T:
    """Call `func`, in a worker thread if this thread is running an event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        in_event_loop = False
    else:
        in_event_loop = True
    if not in_event_loop:
        return func()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(func).result()


def initialize_database_if_needed(
    database_uri: str,
    initialize_database: Callable[[AsyncEngine], Awaitable[None]],
    required_revision: str,
    known_revisions: Iterable[str],
    alembic_ini_template_path: str | Path,
    alembic_dir: str | Path,
) -> bool:
    """
    Create the tables and stamp the head alembic revision, unless already done.

    Returns True if the database was initialized by this call and False if it
    was already initialized. Raises `DatabaseUpgradeNeeded` or
    `UnrecognizedDatabase` as `check_database` does.

    This is a blocking call. Alembic drives its own event loop while stamping,
    so when called from a thread that is itself running an event loop (for
    example, during an ASGI application startup) the work is done in a worker
    thread, using connections private to that thread.

    Parameters
    ----------
    database_uri : str
        A SQLAlchemy database URI with an async driver, e.g. `sqlite+aiosqlite:///catalog.db`.
    initialize_database : async callable
        Creates the tables, given an `AsyncEngine`.
    required_revision, known_revisions : str, Iterable[str]
        The head revision and all known revisions of this database's
        migrations, as passed to `check_database`.
    alembic_ini_template_path, alembic_dir : path
        As passed to `stamp_head`.
    """

    async def do_setup() -> bool:
        engine = create_async_engine(database_uri)
        try:
            await check_database(engine, required_revision, known_revisions)
        except UninitializedDatabase:
            await initialize_database(engine)
            return True
        else:
            return False
        finally:
            await engine.dispose()

    def run() -> bool:
        initialized = asyncio.run(do_setup())
        if initialized:
            stamp_head(alembic_ini_template_path, alembic_dir, database_uri)
        return initialized

    return _run_outside_event_loop(run)


# A migration script can set this module attribute to False to declare that the
# server code at its revision runs correctly on a database at the previous
# revision, so that the migration may be applied after that code is deployed.
REQUIRED_AT_STARTUP = "required_at_startup"


def revisions_accepted_at_startup(
    alembic_dir: Union[str, Path], required_revision: str
) -> list[str]:
    """
    The database revisions at which a server requiring `required_revision` starts.

    These are `required_revision` itself and, if its migration script declares
    `required_at_startup = False`, the revision before it. At most one
    migration may be pending: the declaration is about the code shipped with
    that migration and says nothing about code that is further ahead.
    """
    from alembic.script import ScriptDirectory

    head = ScriptDirectory(str(alembic_dir)).get_revision(required_revision)
    accepted = [required_revision]
    if not getattr(head.module, REQUIRED_AT_STARTUP, True):
        down_revision = head.down_revision
        if isinstance(down_revision, str):
            accepted.append(down_revision)
        elif down_revision is not None:
            accepted.extend(down_revision)
    return accepted
