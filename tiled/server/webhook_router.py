"""
REST API for managing webhooks.

Endpoints
---------
POST   /api/v1/webhooks/target/{path}     Register a webhook on a node
GET    /api/v1/webhooks/target/{path}     List webhooks registered on a node
DELETE /api/v1/webhooks/{webhook_id}      Deactivate / remove a webhook
GET    /api/v1/webhooks/history/{webhook_id}  Recent delivery history

All write endpoints require the ``write:metadata`` scope (same as creating
nodes). Read endpoints require ``read:metadata``.
"""

from typing import List, Optional

import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Security
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND

from ..catalog import orm
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
from ..type_aliases import AccessTags, Scopes

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_catalog_context(entry):
    """Extract the catalog Context from an adapter entry."""
    context = getattr(entry, "context", None)
    if context is None:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail="Webhooks are only supported on catalog-backed trees.",
        )
    return context


def _require_root_context(root_tree):
    """Return the catalog Context from the root tree, or raise 404."""
    ctx = getattr(root_tree, "context", None)
    if ctx is None:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail="Webhooks are only supported on catalog-backed trees.",
        )
    return ctx


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


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_webhook_router() -> APIRouter:
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
        _=Security(check_scopes, scopes=["write:metadata"]),
    ):
        entry = await get_entry(
            path=path,
            security_scopes=["write:metadata"],
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

        # SSRF check: block private/loopback/link-local targets.
        try:
            await asyncio.to_thread(check_url_ssrf_safety, str(body.url))
        except ValueError as exc:
            logger.info("Webhook registration blocked by SSRF check: %s", exc)
            raise HTTPException(
                status_code=400,
                detail="Webhook URL targets a private or reserved address and cannot be registered.",
            ) from exc

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
        response_model=List[WebhookResponse],
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
        _=Security(check_scopes, scopes=["read:metadata"]),
    ):
        entry = await get_entry(
            path=path,
            security_scopes=["read:metadata"],
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
            rows = (
                (
                    await db.execute(
                        select(orm.Webhook).where(orm.Webhook.node_id == entry.node.id)
                    )
                )
                .scalars()
                .all()
            )

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
        _=Security(check_scopes, scopes=["write:metadata"]),
    ):
        root_ctx = _require_root_context(root_tree)

        # Load webhook, verify caller access, and delete
        async with root_ctx.session() as db:
            wh = await db.get(orm.Webhook, webhook_id)
            if wh is None:
                raise HTTPException(
                    status_code=HTTP_404_NOT_FOUND, detail="Webhook not found."
                )
            node_id = wh.node_id

            # Verify the caller has write:metadata access to the webhook's node.
            node_path = await _node_path_from_id(root_ctx, node_id)
            await get_entry(
                path=node_path,
                security_scopes=["write:metadata"],
                principal=principal,
                authn_access_tags=authn_access_tags,
                authn_scopes=authn_scopes,
                root_tree=root_tree,
                session_state=session_state,
                metrics=request.state.metrics,
                structure_families=None,
                access_policy=getattr(request.app.state, "access_policy", None),
            )

            await db.delete(wh)
            await db.commit()

        return {"deleted": webhook_id}

    @router.get(
        "/history/{webhook_id}",
        response_model=List[DeliveryResponse],
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
        _=Security(check_scopes, scopes=["read:metadata"]),
    ):
        root_ctx = _require_root_context(root_tree)

        # Load the webhook to find which node it belongs to.
        async with root_ctx.session() as db:
            wh = await db.get(orm.Webhook, webhook_id)
            if wh is None:
                raise HTTPException(
                    status_code=HTTP_404_NOT_FOUND, detail="Webhook not found."
                )
            node_id = wh.node_id

        # Verify the caller has read:metadata access to the webhook's node.
        node_path = await _node_path_from_id(root_ctx, node_id)
        await get_entry(
            path=node_path,
            security_scopes=["read:metadata"],
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
            root_tree=root_tree,
            session_state=session_state,
            metrics=request.state.metrics,
            structure_families=None,
            access_policy=getattr(request.app.state, "access_policy", None),
        )

        async with root_ctx.session() as db:
            rows = (
                (
                    await db.execute(
                        select(orm.WebhookDelivery)
                        .where(orm.WebhookDelivery.webhook_id == webhook_id)
                        .order_by(orm.WebhookDelivery.time_created.desc())
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )

        return [DeliveryResponse.model_validate(d) for d in rows]

    return router
