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
import re
from typing import Callable

from fastapi import APIRouter, Depends, Request
from strawberry.fastapi import GraphQLRouter

from ..server.authentication import (
    get_current_access_tags,
    get_current_principal,
    get_current_scopes,
)
from ..server.settings import DatabaseSettings
from .schema import schema
from .store import GraphSQLAlchemyStore

logger = logging.getLogger(__name__)

# The query pre-loaded into the GraphiQL editor. It orients newcomers to the
# entity/link graph rather than showing GraphiQL's generic welcome text. Keep
# this free of backticks and `${...}` so it stays a valid JavaScript template
# literal when injected into the IDE HTML below.
DEFAULT_GRAPHIQL_QUERY = """# Tiled — Entity/Link Graph explorer
#
# This endpoint serves the graph of entities (nodes) and the links (edges)
# between them, alongside the catalog tree.
#
# Access is controlled, so most queries need an API key. Open the "Headers"
# tab below and add your key:
#
#     { "Authorization": "Apikey YOUR_API_KEY" }
#
# Without it, queries do not error — they just return empty results.
#
# Run a query with Ctrl-Enter (or the play button). Browse the schema in the
# "Docs" and "Explorer" panels on the left.

query ExploreGraph {
  entities(limit: 10) {
    id
    name
    entityType
    uri
    outgoingLinks(limit: 5) {
      predicate
      object {
        id
        name
      }
    }
  }
  namespaces {
    prefix
    uri
  }
}
"""

# Matches Strawberry's bundled `const EXAMPLE_QUERY = ` ... ` ;` assignment.
_EXAMPLE_QUERY_RE = re.compile(r"const EXAMPLE_QUERY = `.*?`;", re.DOTALL)


class _TiledGraphQLRouter(GraphQLRouter):
    """GraphQLRouter that preloads a Tiled-specific default query.

    Strawberry bundles a static GraphiQL page whose editor opens with a
    generic welcome message. We reuse that page but swap the default query for
    one tailored to the entity/link graph. If Strawberry ever changes the
    template and the marker is not found, the original HTML is served
    unchanged.
    """

    @property
    def graphql_ide_html(self) -> str:
        html = super().graphql_ide_html
        return _EXAMPLE_QUERY_RE.sub(
            lambda _: f"const EXAMPLE_QUERY = `{DEFAULT_GRAPHIQL_QUERY}`;",
            html,
            count=1,
        )


def create_router(get_database_settings: Callable[[], DatabaseSettings]) -> APIRouter:
    store: list[GraphSQLAlchemyStore] = []  # mutable cell — populated on startup

    async def startup() -> None:
        db_settings = get_database_settings()
        logger.info("Initializing links store with shared DB pool: %s", db_settings.uri)
        store.append(await GraphSQLAlchemyStore.from_database_settings(db_settings))

    async def shutdown() -> None:
        if store:
            await store[0].close()
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

    graphql_router = _TiledGraphQLRouter(
        schema,
        context_getter=get_context,
        graphql_ide="graphiql",
    )

    router = APIRouter(on_startup=[startup], on_shutdown=[shutdown])
    router.include_router(graphql_router, prefix="/api/graphql")

    return router
