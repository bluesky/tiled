"""
Links router for the Tiled service.

Provides a GraphQL interface for the entity/link graph under /api/v1/links.
The store lifecycle is owned by the router: startup/shutdown handlers are
registered automatically when the router is included in a FastAPI app.

Database migrations are NOT run here — they are the responsibility of the
caller (app startup) following the same pattern as the authn and catalog
databases. The links tables are managed by catalog migrations, so use
`tiled catalog init` / `upgrade-database` for the target database, or let
the server auto-initialize when database_init_if_not_exists is set.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import APIRouter, Depends, Request
from strawberry.fastapi import GraphQLRouter

from ..server.authentication import (
    get_current_access_tags,
    get_current_principal,
    get_current_scopes,
)
from .schema import schema
from .store import SQLAlchemyStore as SQLiteStore
from .store import Store, _url_from_path

logger = logging.getLogger(__name__)


def create_router(db_path: Optional[str] = None) -> APIRouter:
    store: list[Store] = []  # mutable cell — populated on startup

    async def startup() -> None:
        resolved = db_path or os.environ.get("SPLASH_LINKS_DB", ":memory:")
        db_url = _url_from_path(resolved)
        logger.info("Initializing links store: %s", db_url)
        store.append(SQLiteStore(db_url))

    async def shutdown() -> None:
        if store:
            store[0].close()
            logger.info("Links store closed")

    async def get_context(
        request: Request,
        principal=Depends(get_current_principal),
        authn_access_tags=Depends(get_current_access_tags),
        authn_scopes=Depends(get_current_scopes),
    ) -> dict:
        return {
            "store": store[0],
            "principal": principal,
            "authn_access_tags": authn_access_tags,
            "authn_scopes": authn_scopes,
            "access_policy": getattr(request.app.state, "access_policy", None),
        }

    graphql_router = GraphQLRouter(
        schema,
        context_getter=get_context,
        graphql_ide="graphiql",
    )

    router = APIRouter(on_startup=[startup], on_shutdown=[shutdown])
    router.include_router(graphql_router, prefix="/api/graphql")

    return router
