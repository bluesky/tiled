from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from ..alembic_utils import DatabaseUpgradeNeeded, UninitializedDatabase, check_database
from .base import Base

# This is list of all valid revisions (from current to oldest).
ALL_REVISIONS = [
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
        # auto-registration of principal tags (register_principal_tag_rows).
        # On a server without an access policy, tags are ignored and
        # these rows are inert.
        public_tag_id = await connection.scalar(
            select(orm.AccessTag.id).where(orm.AccessTag.name == "public")
        )
        if public_tag_id is None:
            result = await connection.execute(
                insert(orm.AccessTag).values(name="public", is_public=True)
            )
            public_tag_id = result.inserted_primary_key[0]
        root_is_tagged = await connection.scalar(
            select(orm.NodeAccessTag.tag_id).where(
                orm.NodeAccessTag.node_id == 0,
                orm.NodeAccessTag.tag_id == public_tag_id,
            )
        )
        if root_is_tagged is None:
            await connection.execute(
                insert(orm.NodeAccessTag).values(node_id=0, tag_id=public_tag_id)
            )
        if engine.dialect.name == "sqlite":
            # Use write-ahead log mode. This persists across all future connections
            # until/unless manually switched.
            # https://www.sqlite.org/wal.html
            await connection.execute(text("PRAGMA journal_mode=WAL;"))
        await connection.commit()


async def register_principal_tag_rows(connection, access_tag_names):
    """
    Auto-register bare access tag rows for any principal tags in
    access_tag_names. Principal tags need to exist at write, possibly before
    the access tags compiler has been able to create them.

    Call this on the same connection/transaction as the tag assignment, so
    that the row is never observed unassigned and the compiler's retention
    rules will never delete it. (An AsyncSession caller can pass
    `await session.connection()`.)
    """
    from ..access_control.protocols import PRINCIPAL_TAG_PREFIXES
    from . import orm

    principal_tags = {
        name for name in access_tag_names if name.startswith(PRINCIPAL_TAG_PREFIXES)
    }
    if not principal_tags:
        return
    if connection.dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as upsert
    else:
        from sqlalchemy.dialects.sqlite import insert as upsert
    await connection.execute(
        upsert(orm.AccessTag.__table__)
        .values([{"name": name, "is_public": False} for name in sorted(principal_tags)])
        .on_conflict_do_nothing(index_elements=["name"])
    )


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
