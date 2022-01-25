from alembic import command
from alembic.config import Config
from alembic.runtime import migration
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from .alembic_utils import temp_alembic_ini

Base = declarative_base()

# This is the alembic revision ID of the database revision
# required by this version of Tiled.
REQUIRED_REVISION = "481830dd6c11"
# This is set of all valid revisions.
ALL_REVISIONS = {"481830dd6c11"}


def create_default_roles(engine):
    # Avoid circular import.

    from .orm import Role

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    db.add(
        Role(
            name="user",
            description="Default Role for users.",
            scopes=["read:metadata", "read:data", "apikeys"],
        ),
    )
    db.add(
        Role(
            name="admin",
            description="Default Role for services.",
            scopes=[
                "read:metadata",
                "read:data",
                "admin:apikeys",
                "read:principals",
                "metrics",
            ],
        ),
    )
    db.commit()


def initialize_database(engine):

    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    # Create all tables.
    Base.metadata.create_all(engine)

    # Initialize Roles table.
    create_default_roles(engine)

    # Mark current revision.
    with temp_alembic_ini(engine.url) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        command.stamp(alembic_cfg, "head")


class UnrecognizedDatabase(Exception):
    pass


class UninitializedDatabase(Exception):
    pass


class DatabaseUpgradeNeeded(Exception):
    pass


def get_current_revision(engine):
    with engine.begin() as conn:
        context = migration.MigrationContext.configure(conn)
        heads = context.get_current_heads()
        if heads == ():
            return None
        elif len(heads) != 1:
            raise UnrecognizedDatabase(
                f"This database {engine.url} is stamped with an alembic revisions {heads}. "
                "It looks like Tiled has been configured to connect to a database "
                "already populated by some other application (not Tiled) or else "
                "its database is in a corrupted state."
            )
        (revision,) = heads
        if revision not in ALL_REVISIONS:
            raise UnrecognizedDatabase(
                f"The datbase {engine.url} has an unrecognized revision {revision}. "
                "It may have been created by a newer version of Tiled."
            )
        return revision


def check_database(engine):
    revision = get_current_revision(engine)
    if revision is None:
        raise UninitializedDatabase(
            f"The database {engine.url} has no revision stamp. It may be empty. "
            "It can be initialized with `initialize_database(engine)`."
        )
    elif revision != REQUIRED_REVISION:
        raise DatabaseUpgradeNeeded(
            f"The database {engine.url} has revision {revision} and "
            f"needs to be upgraded to revision {REQUIRED_REVISION}."
        )
