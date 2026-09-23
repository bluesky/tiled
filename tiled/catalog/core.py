from sqlalchemy import literal, select, text
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from ..access_control.protocols import normalize_access_tags
from ..alembic_utils import DatabaseUpgradeNeeded, UninitializedDatabase, check_database
from ..utils import UndefinedAccessTags
from . import orm
from .base import Base

# This is list of all valid revisions (from current to oldest).
ALL_REVISIONS = [
    "3e87cbeb195b",
    "0d4e1f2a3b4c",
    "3bc110ce44e9",
    "de302a096358",
    "c31f6a1d7e20",
    "9bc9b57294b9",
    "b93c79d197f4",
    "e8956581ecd5",
    "bf2fe0eb8ee8",
    "85a47342e78e",
    "4cf6011a8db5",
    "a86a48befff6",
    "dfbb7478c6bd",
    "a963a6c32a0c",
    "e05e918092c3",
    "7809873ea2c7",
    "9331ed94d6ac",
    "45a702586b2a",
    "ed3a4223a600",
    "e756b9381c14",
    "2ca16566d692",
    "1cd99c02d0c7",
    "a66028395cab",
    "3db11ff95b6c",
    "0b033e7fbe30",
    "83889e049ddc",
    "6825c778aa3c",
]
REQUIRED_REVISION = ALL_REVISIONS[0]


async def initialize_database(engine: AsyncEngine):
    from ..graph import orm as graph_orm  # noqa: F401
    from . import orm  # noqa: F401

    async with engine.connect() as connection:
        # Install extensions
        if engine.dialect.name == "postgresql":
            await connection.execute(text("create extension btree_gin;"))
        # Create all tables.
        await connection.run_sync(Base.metadata.create_all)
        # The persisted catalog root node (nodes.id = 0, inserted by the
        # nodes_closure DDL listener) is always tagged 'public': under
        # tag-based access control, a node with no tags is inaccessible, and
        # the root must never block traversal to its children. The
        # association requires the 'public' tag row to exist (foreign key),
        # so it is created here if missing. Besides the access tags compiler,
        # tag rows are inserted only here and by the storage layers'
        # auto-registration of tags on first use (get_or_create_tag_ids).
        # On a server without an access policy, tags are ignored and
        # these rows are inert.
        upsert = dialect_insert(engine)
        await connection.execute(
            upsert(orm.AccessTag.__table__)
            .values(name="public", is_public=True)
            .on_conflict_do_nothing(index_elements=["name"])
        )
        # The tag id is resolved in SQL, so no round-trip is needed and the
        # statement is a no-op when the association already exists.
        await connection.execute(
            upsert(orm.NodeAccessTagAssociation.__table__)
            .from_select(
                ["node_id", "tag_id"],
                select(literal(0), orm.AccessTag.id).where(
                    orm.AccessTag.name == "public"
                ),
            )
            .on_conflict_do_nothing(index_elements=["node_id", "tag_id"])
        )
        if engine.dialect.name == "sqlite":
            # Use write-ahead log mode. This persists across all future connections
            # until/unless manually switched.
            # https://www.sqlite.org/wal.html
            await connection.execute(text("PRAGMA journal_mode=WAL;"))
        await connection.commit()


def dialect_insert(bind):
    """
    Return the dialect's INSERT construct, e.g. postgresql.insert.

    SQLAlchemy's generic insert() has no ON CONFLICT clause; only the
    PostgreSQL and SQLite constructs do, and they emit the same SQL.
    """
    if bind.dialect.name == "postgresql":
        return postgresql_insert
    return sqlite_insert


async def _find_tag_ids(connection, names):
    "Map each of names that has an access_tags row to the row's id."
    if not names:
        return {}
    tags = orm.AccessTag.__table__
    statement = select(tags.c.name, tags.c.id).where(tags.c.name.in_(names))
    return dict((await connection.execute(statement)).all())


def _raise_if_undefined(names, found):
    undefined = names - found.keys()
    if undefined:
        raise UndefinedAccessTags(
            f"Cannot apply access tags that are not defined: {sorted(undefined)}"
        )


async def get_tag_ids(connection, access_tag_names):
    """
    Resolve access tag names to access_tags ids, e.g. ["public"] -> [1].

    Raises UndefinedAccessTags naming any tag that has no row.
    """
    names = normalize_access_tags(access_tag_names)
    found = await _find_tag_ids(connection, names)
    _raise_if_undefined(names, found)
    return list(found.values())


async def get_or_create_tag_ids(connection, access_tag_names):
    """
    Like get_tag_ids, but first create rows for tags that have none.

    Only for tags an access policy approved. Nodes, entities, and links
    reference tags by row id, but the policy decides which tags are valid:
    principal tags can precede the compiler's next run, and an external
    policy has no compiler. A bare row grants nothing, so creating it on
    first use is safe. Tags from config, like mount nodes', go through
    get_tag_ids instead, so a typo fails rather than creating a tag.

    Call this on the same connection/transaction as the tag assignment, so
    that the row is never observed unassigned and the compiler's retention
    rules will never delete it. (An AsyncSession caller can pass
    `await session.connection()`.)

    Raises UndefinedAccessTags for names longer than the column, which can
    never have a row.
    """
    # Rejects a bare string, which set() would split into one tag per letter.
    names = normalize_access_tags(access_tag_names)
    found = await _find_tag_ids(connection, names)
    missing = sorted(names - found.keys())
    if not missing:
        return list(found.values())

    tags = orm.AccessTag.__table__
    max_length = tags.c.name.type.length
    too_long = [name for name in missing if len(name) > max_length]
    if too_long:
        raise UndefinedAccessTags(
            f"Access tag names longer than {max_length} characters: {too_long}"
        )

    # Insert only missing names: on PostgreSQL, ON CONFLICT DO NOTHING
    # consumes an id even when the row exists. Sorted, so concurrent
    # writers insert new names in one order (no deadlock between writers).
    await connection.execute(
        dialect_insert(connection)(tags)
        .values([{"name": name, "is_public": False} for name in missing])
        .on_conflict_do_nothing(index_elements=["name"])
    )
    found.update(await _find_tag_ids(connection, missing))

    # Raise rather than silently drop a tag whose row a compile just deleted.
    _raise_if_undefined(names, found)
    return list(found.values())


async def check_catalog_database(engine: AsyncEngine):
    redacted_url = engine.url._replace(password="[redacted]")
    try:
        await check_database(engine, REQUIRED_REVISION, ALL_REVISIONS)
    except UninitializedDatabase:
        raise UninitializedDatabase(
            f"""

No catalog database found at {redacted_url}

To create one, run:

tiled catalog init {redacted_url}
""",
        )
    except DatabaseUpgradeNeeded:
        raise DatabaseUpgradeNeeded(
            f"""

The catalog found at

{redacted_url}

was created using an older version of Tiled. It needs to be upgraded
to work with this version. Back up the database, and the run:

tiled catalog upgrade-database {redacted_url}
""",
        )
