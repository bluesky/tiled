from sqlalchemy.ext.asyncio import AsyncEngine

from .store import _metadata

ALL_REVISIONS = ["7f3a9d1c0b25"]
REQUIRED_REVISION = ALL_REVISIONS[0]


async def initialize_database(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(_metadata.create_all)
