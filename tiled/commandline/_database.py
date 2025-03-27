import asyncio
from abc import ABC
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, Field
from pydantic_settings import CliApp, CliSubCommand
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from ..alembic_utils import (
    UninitializedDatabase,
    check_database,
    downgrade,
    get_current_revision,
    stamp_head,
    upgrade,
)
from ..authn_database.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
from ..authn_database.core import ALL_REVISIONS, REQUIRED_REVISION, initialize_database
from ..utils import ensure_specified_sql_driver


class DatabaseCommand(ABC, BaseModel):
    database_uri: Annotated[str, BeforeValidator(ensure_specified_sql_driver)]

    def get_engine(self) -> AsyncEngine:
        return create_async_engine(self.database_uri)

    def redacted_url(self, engine: AsyncEngine) -> str:
        return engine.url._replace(password="[redacted]")


class Initialize(DatabaseCommand):
    """
    Initialize a SQL database for use by Tiled.
    """

    def cli_cmd(self) -> None:
        async def inner():
            engine = self.get_engine()
            redacted_url = self.redacted_url(engine)
            try:
                await check_database(engine, REQUIRED_REVISION, ALL_REVISIONS)
            except UninitializedDatabase:
                # Create tables and stamp (alembic) revision.
                print(
                    f"Database {redacted_url} is new. Creating tables and marking revision {REQUIRED_REVISION}.",
                )
                await initialize_database(engine)
                print("Database initialized.")
            else:
                print(f"Database at {redacted_url} is already initialized.")
                raise ValueError
            await engine.dispose()

        asyncio.run(inner())
        stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, self.database_uri)


class Upgrade(DatabaseCommand):
    revision: Annotated[
        str,
        "The ID of a revision to upgrade to. By default, upgrade to the latest one.",
    ] = "head"
    """
    Upgrade the database schema to the latest or a specified version.
    """

    async def cli_cmd(self) -> None:
        async def inner():
            engine = self.get_engine()
            redacted_url = self.redacted_url(engine)
            current_revision = await get_current_revision(engine, ALL_REVISIONS)
            await engine.dispose()
            if current_revision is None:
                raise UninitializedDatabase(
                    f"Database {redacted_url} has not been initialized. Use `tiled admin database init`."
                )

        asyncio.run(inner())
        upgrade(
            ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, self.database_uri, self.revision
        )


class Downgrade(DatabaseCommand):
    revision: Annotated[str, "The ID of a revision to downgrade to."]
    """
    Downgrade the database schema to a specified version.
    """

    def cli_cmd(self) -> None:
        async def inner():
            engine = create_async_engine(self.database_uri)
            redacted_url = engine.url._replace(password="[redacted]")
            current_revision = await get_current_revision(engine, ALL_REVISIONS)
            if current_revision is None:
                raise UninitializedDatabase(
                    f"Database {redacted_url} has not been initialized. Use `tiled admin database init`."
                )

        asyncio.run(inner())
        downgrade(
            ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, self.database_uri, self.revision
        )


class Database(BaseModel):
    initialize: CliSubCommand[Initialize] = Field(alias="init")
    upgrade: CliSubCommand[Upgrade]
    downgrade: CliSubCommand[Downgrade]

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)
