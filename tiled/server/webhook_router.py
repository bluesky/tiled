"""
REST API for managing webhooks.

Endpoints
---------
POST   /api/v1/webhooks/target/{path}     Register a webhook on a node
GET    /api/v1/webhooks/target/{path}     List webhooks registered on a node
DELETE /api/v1/webhooks/{webhook_id}      Deactivate / remove a webhook
GET    /api/v1/webhooks/history/{webhook_id}  Recent delivery history

All write endpoints require the ``write:webhooks`` scope.
Read endpoints require ``read:webhooks``.
Both scopes are granted to admin users only.
"""

import asyncio
import logging
from typing import Callable, Coroutine, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Security
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND

from ..catalog import orm
from ..config import WebhooksConfig
from ..type_aliases import AccessTags, Scopes
from .authentication import (
    check_scopes,
    get_current_access_tags,
    get_current_principal,
    get_current_scopes,
    get_session_state,
)
from .dependencies import get_entry, get_root_tree
from .schemas import (
    DeliveryResponse,
    Principal,
    WebhookRegistrationRequest,
    WebhookResponse,
)
from .webhooks import _encrypt_secret, check_url_ssrf_safety

logger = logging.getLogger(__name__)

# Type alias for a URL validator: accepts a WebhookRegistrationRequest,
# raises HTTPException on failure, returns None on success.
UrlValidator = Callable[[WebhookRegistrationRequest], Coroutine]


async def _noop_url_validator(body: WebhookRegistrationRequest) -> None:
    """No-op URL validator for development use (e.g. SimpleTiledServer).

    Skips HTTPS enforcement and SSRF checks so that local HTTP endpoints
    (such as http://localhost:9000) can be used as webhook targets.
    """
    pass


def _build_url_validator(config: WebhooksConfig) -> UrlValidator:
    """Build a URL validator from the webhooks configuration.

    Returns a validator that conditionally enforces HTTPS and SSRF checks
    based on the ``allow_http`` and ``allow_private_addresses`` settings.
    With the default config (both ``False``), this is the production
    validator that enforces HTTPS and blocks SSRF targets.
    """
    if config.allow_http and config.allow_private_addresses:
        logger.warning(
            "Webhook URL validation is fully relaxed: "
            "allow_http and allow_private_addresses are both enabled."
        )
        return _noop_url_validator

    if config.allow_http:
        logger.warning("Webhook HTTPS enforcement is disabled (allow_http=True).")
    if config.allow_private_addresses:
        logger.warning(
            "Webhook SSRF protection is disabled (allow_private_addresses=True)."
        )

    async def _url_validator(
        body: WebhookRegistrationRequest,
    ) -> None:
        if not config.allow_http:
            try:
                body.check_https()
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        if not config.allow_private_addresses:
            try:
                await asyncio.to_thread(check_url_ssrf_safety, str(body.url))
            except ValueError as exc:
                logger.info("Webhook registration blocked by SSRF check: %s", exc)
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Webhook URL targets a private or reserved address "
                        "and cannot be registered."
                    ),
                ) from exc

    return _url_validator


def _get_catalog_context(entry):
    """Extract the catalog Context from an adapter entry, or raise 404."""
    context = getattr(entry, "context", None)
    if context is None:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail="Webhooks are only supported on catalog-backed trees.",
        )
    return context


async def _node_path_from_id(ctx, node_id: int) -> str:
    """Return the slash-joined path string for a node given its internal DB id.

    Replicates the ``path_segments`` query used in CatalogNodeAdapter so the
    router can perform access-control checks without holding an adapter reference.
    """
    async with ctx.session() as db:
        stmt = (
            select(orm.Node.key)
            .join(orm.NodesClosure, orm.NodesClosure.ancestor == orm.Node.id)
            .where(orm.NodesClosure.descendant == node_id)
            .where(orm.Node.id != 0)
            .order_by(orm.NodesClosure.depth.desc())
        )
        keys = (await db.execute(stmt)).scalars().all()
    return "/".join(keys)


def _iter_catalog_contexts(tree, prefix: tuple = ()):
    """Yield ``(mount_prefix, catalog_context)`` pairs reachable from ``tree``.

    A catalog-backed adapter exposes a ``context`` attribute.  When catalogs are
    mounted under sub-paths (the ``trees:`` config form), the root is a
    ``MapAdapter`` whose children are the mounted trees; descend into it to find
    each catalog and remember the path prefix it is mounted at.
    """
    context = getattr(tree, "context", None)
    if context is not None:
        yield prefix, context
        return
    mapping = getattr(tree, "_mapping", None)
    if mapping:
        for key, child in mapping.items():
            yield from _iter_catalog_contexts(child, prefix + (key,))


async def _resolve_webhook_context(root_tree, webhook_id: int):
    """Locate the catalog context owning ``webhook_id`` and load the webhook.

    Returns ``(mount_prefix, context, webhook)``.  Searches every catalog
    reachable from the root so that both root-mounted and sub-path-mounted
    catalogs are supported.  Raises 404 if no catalog exists or the webhook is
    not found in any of them.
    """
    contexts = list(_iter_catalog_contexts(root_tree))
    if not contexts:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail="Webhooks are only supported on catalog-backed trees.",
        )
    for prefix, ctx in contexts:
        async with ctx.session() as db:
            wh = await db.get(orm.Webhook, webhook_id)
            if wh is not None:
                return prefix, ctx, wh
    raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Webhook not found.")


def _global_node_path(prefix: tuple, local_path: str) -> str:
    """Join a catalog's mount prefix with a node path local to that catalog."""
    local_segments = local_path.split("/") if local_path else []
    return "/".join([*prefix, *local_segments])


def get_webhook_router(
    webhook_settings: WebhooksConfig,
    webhook_url_validator: UrlValidator | None = None,
) -> APIRouter:
    if webhook_url_validator is None:
        webhook_url_validator = _build_url_validator(webhook_settings)

    router = APIRouter(prefix="/webhooks")

    @router.post(
        "/target/{path:path}",
        response_model=WebhookResponse,
        summary="Register a webhook on a node",
    )
    async def register_webhook(
        request: Request,
        path: str,
        body: WebhookRegistrationRequest,
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[AccessTags] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["write:webhooks"]),
    ):
        entry = await get_entry(
            path=path,
            security_scopes=["write:webhooks"],
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
            root_tree=root_tree,
            session_state=session_state,
            metrics=request.state.metrics,
            structure_families=None,
            access_policy=getattr(request.app.state, "access_policy", None),
        )
        ctx = _get_catalog_context(entry)

        await webhook_url_validator(body)

        encrypted_secret: Optional[str] = None
        if body.secret:
            # Use the same key source as the dispatcher (webhooks.secret_keys
            # from config, injected into the catalog context at startup).
            secret_keys = getattr(ctx, "webhook_secret_keys", None) or []
            if not secret_keys:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Webhook secrets cannot be stored: no secret_keys are configured "
                        "in the webhooks section of the Tiled configuration. "
                        "Add webhook_secret_keys or omit the secret field."
                    ),
                )
            encrypted_secret = _encrypt_secret(body.secret, secret_keys)

        async with ctx.session() as db:
            wh = orm.Webhook(
                node_id=entry.node.id,
                url=str(body.url),
                secret=encrypted_secret,
                events=body.events or None,
                active=True,
            )
            db.add(wh)
            await db.commit()
            await db.refresh(wh)

        return WebhookResponse.model_validate(wh)

    @router.get(
        "/target/{path:path}",
        response_model=list[WebhookResponse],
        summary="List webhooks registered on a node",
    )
    async def list_webhooks(
        request: Request,
        path: str,
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[AccessTags] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["read:webhooks"]),
    ):
        entry = await get_entry(
            path=path,
            security_scopes=["read:webhooks"],
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
            root_tree=root_tree,
            session_state=session_state,
            metrics=request.state.metrics,
            structure_families=None,
            access_policy=getattr(request.app.state, "access_policy", None),
        )
        ctx = _get_catalog_context(entry)

        async with ctx.session() as db:
            result = await db.execute(
                select(orm.Webhook).where(orm.Webhook.node_id == entry.node.id)
            )
            rows = result.scalars().all()

        return [WebhookResponse.model_validate(wh) for wh in rows]

    @router.delete(
        "/{webhook_id}",
        summary="Deactivate and delete a webhook",
    )
    async def delete_webhook(
        request: Request,
        webhook_id: int,
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[AccessTags] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["write:webhooks"]),
    ):
        # Locate the catalog that owns this webhook (supports catalogs mounted
        # at the root or under sub-paths).
        prefix, ctx, wh = await _resolve_webhook_context(root_tree, webhook_id)
        node_id = wh.node_id

        # Verify the caller has write:webhooks access to the webhook's node.
        local_path = await _node_path_from_id(ctx, node_id)
        node_path = _global_node_path(prefix, local_path)
        await get_entry(
            path=node_path,
            security_scopes=["write:webhooks"],
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
            root_tree=root_tree,
            session_state=session_state,
            metrics=request.state.metrics,
            structure_families=None,
            access_policy=getattr(request.app.state, "access_policy", None),
        )

        async with ctx.session() as db:
            wh = await db.get(orm.Webhook, webhook_id)
            if wh is not None:
                await db.delete(wh)
                await db.commit()

        return {"deleted": webhook_id}

    @router.get(
        "/history/{webhook_id}",
        response_model=list[DeliveryResponse],
        summary="Delivery history for a webhook",
    )
    async def webhook_history(
        request: Request,
        webhook_id: int,
        limit: int = Query(50, ge=1, le=500),
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[AccessTags] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["read:webhooks"]),
    ):
        # Locate the catalog that owns this webhook (supports catalogs mounted
        # at the root or under sub-paths).
        prefix, ctx, wh = await _resolve_webhook_context(root_tree, webhook_id)
        node_id = wh.node_id

        # Verify the caller has read:webhooks access to the webhook's node.
        local_path = await _node_path_from_id(ctx, node_id)
        node_path = _global_node_path(prefix, local_path)
        await get_entry(
            path=node_path,
            security_scopes=["read:webhooks"],
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
            root_tree=root_tree,
            session_state=session_state,
            metrics=request.state.metrics,
            structure_families=None,
            access_policy=getattr(request.app.state, "access_policy", None),
        )

        async with ctx.session() as db:
            result = await db.execute(
                select(orm.WebhookDelivery)
                .where(orm.WebhookDelivery.webhook_id == webhook_id)
                .order_by(orm.WebhookDelivery.time_created.desc())
                .limit(limit)
            )
            rows = result.scalars().all()

        return [DeliveryResponse.model_validate(d) for d in rows]

    return router
