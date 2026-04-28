"""
DB-backed webhook dispatcher for Tiled server events.

Webhooks are registered per-node via the REST API:

    POST /api/v1/webhooks/target/{path}   - register
    GET  /api/v1/webhooks/target/{path}   - list
    DELETE /api/v1/webhooks/{webhook_id}  - remove

When a catalog event fires (e.g. container-child-created), Tiled:
1. Writes a WebhookDelivery row (outcome="pending") to the catalog DB.
2. Schedules an asyncio background task that POSTs to the target URL.
3. Updates the row with the outcome (status_code, attempts, delivered_at).

Delivery is retried up to MAX_ATTEMPTS times with exponential back-off.
"""

import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import logging
import socket
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse

import httpx
import stamina
from sqlalchemy import delete, select

from ..catalog import orm
from ..server.schemas import DeliveryOutcome, WebhookEvent

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
RETRY_WAIT_INITIAL = 1.0  # first inter-attempt wait (seconds)
RETRY_WAIT_EXP_BASE = 5.0  # base for exponential growth: 1 s → 5 s → 25 s …
RETRY_WAIT_MAX = 30.0  # cap (seconds)

# Maximum number of concurrent outbound webhook deliveries.
_DISPATCH_CONCURRENCY = 32

# Log a warning when this many tasks are outstanding (memory pressure indicator).
_PENDING_TASK_WARN_THRESHOLD = 200

# Delivery row pruning: run once per day, delete rows older than 30 days.
_DELIVERY_PRUNE_INTERVAL = 86_400  # seconds
_DELIVERY_PRUNE_FAILURE_WAIT = 60  # seconds; retry sooner after a failure
_DELIVERY_MAX_AGE_DAYS = 30

# --- Retry helper ---


class _DeliveryHTTPError(Exception):
    """Raised by _deliver when the remote returns a non-2xx response.

    Treated as a retryable condition so stamina will back off and retry.
    Carries the HTTP status code so it can be stored after all attempts.
    """

    def __init__(self, detail: str, status_code: int) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


# --- SSRF protection — blocklist ---
# Webhook delivery makes outbound HTTP requests to user-supplied URLs, which
# creates a Server-Side Request Forgery (SSRF) risk.  The blocklist below
# blocks well-known private/internal ranges at registration time.
#
# IMPORTANT: this is NOT sufficient on its own.  DNS-rebinding attacks can
# bypass hostname-level checks: a hostname may pass validation here and later
# resolve to an internal address when the actual HTTP request is made.
#
# For production, also configure a network-level egress proxy that blocks
# private ranges at the TCP layer, where DNS rebinding cannot help an attacker:
#   - Smokescreen  (https://github.com/stripe/smokescreen)
#   - Squid        (http://www.squid-cache.org/)
#   - Envoy        (https://www.envoyproxy.io/)
#
# Set HTTP_PROXY / HTTPS_PROXY env vars (httpx respects them) to route all
# outbound webhook requests through such a proxy.
# ---------------------------------------------------------------------------

# Explicit blocklist of private/reserved ranges.  Using explicit ranges rather
# than relying solely on ipaddress.is_private gives consistent behaviour across
# Python 3.10–3.12 (is_private semantics changed in 3.11).
_BLOCKED_NETWORKS: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = [
    ipaddress.ip_network("127.0.0.0/8"),  # IPv4 loopback
    ipaddress.ip_network("10.0.0.0/8"),  # RFC 1918
    ipaddress.ip_network("172.16.0.0/12"),  # RFC 1918
    ipaddress.ip_network("192.168.0.0/16"),  # RFC 1918
    ipaddress.ip_network("169.254.0.0/16"),  # link-local / cloud metadata
    ipaddress.ip_network("100.64.0.0/10"),  # shared address space (CGNAT)
    ipaddress.ip_network("0.0.0.0/8"),  # "this" network
    ipaddress.ip_network("::1/128"),  # IPv6 loopback
    ipaddress.ip_network("fc00::/7"),  # IPv6 unique local
    ipaddress.ip_network("fe80::/10"),  # IPv6 link-local
]


def check_url_ssrf_safety(url: str) -> None:
    """Raise ``ValueError`` if *url* resolves to a private/loopback/reserved address.

    Call this at webhook registration time.  Note that DNS-rebinding attacks can
    still bypass hostname-level checks; see module-level comment for the full
    mitigation strategy.

    Parameters
    ----------
    url:
        The webhook target URL to validate.

    Raises
    ------
    ValueError
        If the URL hostname resolves to any address in ``_BLOCKED_NETWORKS``.
    """
    parsed = urlparse(url)
    hostname = parsed.hostname
    if not hostname:
        raise ValueError(f"Cannot parse hostname from URL: {url!r}")
    try:
        # getaddrinfo returns all addresses (both A and AAAA records).
        infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror as exc:
        raise ValueError(
            f"Cannot resolve webhook URL hostname {hostname!r}: {exc}"
        ) from exc
    for _family, _type, _proto, _canonname, sockaddr in infos:
        ip_str = sockaddr[0]
        try:
            addr = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        for net in _BLOCKED_NETWORKS:
            if addr in net:
                raise ValueError(
                    f"Webhook URL {url!r} resolves to {addr}, which is in the "
                    f"blocked network {net} (private/loopback/reserved). "
                    "Configure a network-level egress proxy for production use."
                )


# --- Low-level: signing helper and secret encryption/decryption ---


def _sign(body: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _make_multi_fernet(secret_keys: list[str]):
    """Derive a MultiFernet cipher from Tiled secret_key strings.

    The first key is used for encryption; all keys are tried for decryption,
    enabling transparent key rotation alongside JWT key rotation.
    """
    from cryptography.fernet import Fernet, MultiFernet

    fernets = [
        Fernet(base64.urlsafe_b64encode(hashlib.sha256(k.encode()).digest()))
        for k in secret_keys
    ]
    return MultiFernet(fernets)


def _encrypt_secret(plaintext: str, secret_keys: list[str]) -> str:
    """Encrypt a webhook HMAC signing secret for storage at rest."""
    return _make_multi_fernet(secret_keys).encrypt(plaintext.encode()).decode()


def _decrypt_secret(ciphertext: str, secret_keys: list[str]) -> str | None:
    """Decrypt a stored webhook HMAC signing secret.

    Returns None and logs an error if decryption fails (e.g. wrong key).
    """
    from cryptography.fernet import InvalidToken

    try:
        return _make_multi_fernet(secret_keys).decrypt(ciphertext.encode()).decode()
    except InvalidToken:
        logger.error(
            "Failed to decrypt webhook secret – verify secret_keys configuration "
            "matches the key used when the webhook was registered."
        )
        return None


# --- Delivery task: runs in the asyncio event loop background ---


async def _deliver(
    *,
    client: httpx.AsyncClient,
    session_factory,
    delivery_id: int,
    url: str,
    secret: str | None,
    event_id: str,
    payload: dict,
) -> None:
    """
    Attempt delivery up to MAX_ATTEMPTS times, updating the WebhookDelivery
    row after every attempt.
    """
    body = json.dumps(payload).encode()
    headers = {
        "Content-Type": "application/json",
        # Consumers can use this header to deduplicate retried deliveries.
        "X-Tiled-Event-ID": event_id,
    }
    if secret:
        headers["X-Tiled-Signature"] = _sign(body, secret)

    status_code: int | None = None
    error_detail: str | None = None
    last_attempt = 0

    try:
        async for attempt in stamina.retry_context(
            on=Exception,
            attempts=MAX_ATTEMPTS,
            wait_initial=RETRY_WAIT_INITIAL,
            wait_max=RETRY_WAIT_MAX,
            wait_exp_base=RETRY_WAIT_EXP_BASE,
            wait_jitter=RETRY_WAIT_INITIAL,  # add randomness to avoid thundering herd
            timeout=None,
        ):
            last_attempt += 1
            with attempt:
                resp = await client.post(
                    url, content=body, headers=headers, timeout=10.0
                )
                status_code = resp.status_code
                if not resp.is_success:
                    raise _DeliveryHTTPError(
                        f"HTTP {resp.status_code}: {resp.text[:256]}",
                        resp.status_code,
                    )
    except _DeliveryHTTPError as exc:
        error_detail = exc.detail
    except Exception as exc:
        error_detail = repr(exc)[:256]

    outcome = (
        DeliveryOutcome.success
        if (status_code and 200 <= status_code < 300)
        else DeliveryOutcome.failed
    )

    async with session_factory() as db:
        delivery = await db.get(orm.WebhookDelivery, delivery_id)
        if delivery is not None:
            delivery.status_code = status_code
            delivery.attempts = last_attempt
            delivery.outcome = outcome
            delivery.error_detail = error_detail
            delivery.delivered_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)
            await db.commit()

    if outcome == DeliveryOutcome.failed:
        logger.warning(
            "Webhook delivery %d to %s failed after %d attempt(s): %s",
            delivery_id,
            url,
            last_attempt,
            error_detail,
        )


async def _prune_old_deliveries(
    session_factory, interval: int, max_age_days: int
) -> None:
    """Background task: periodically delete old WebhookDelivery rows.

    Waits *interval* seconds between successful runs.  After a failure it
    retries after ``_DELIVERY_PRUNE_FAILURE_WAIT`` seconds so that a transient
    DB outage does not delay pruning by a full day.
    """
    wait = interval
    while True:
        await asyncio.sleep(wait)
        cutoff = datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(
            days=max_age_days
        )
        try:
            async with session_factory() as db:
                await db.execute(
                    delete(orm.WebhookDelivery).where(
                        orm.WebhookDelivery.time_created < cutoff
                    )
                )
                await db.commit()
            logger.debug(
                "Pruned webhook_deliveries rows older than %d days.", max_age_days
            )
            wait = interval  # reset to normal cadence after success
        except Exception:
            logger.warning("Failed to prune webhook_deliveries.", exc_info=True)
            wait = _DELIVERY_PRUNE_FAILURE_WAIT  # retry sooner after failure


# --- Dispatcher: attached to the catalog Context at startup ---


class WebhookDispatcher:
    """
    Dispatches webhook events to all matching registered webhooks for a node.

    Parameters
    ----------
    session_factory : callable
        Callable returning an ``AsyncSession`` context manager
        (i.e. ``context.session``).
    client : httpx.AsyncClient, optional
        Injected for testing.
    """

    def __init__(
        self,
        session_factory,
        _client: httpx.AsyncClient | None = None,
        secret_keys: list[str] | None = None,
    ):
        self._session_factory = session_factory
        self._client = _client
        self._owns_client = _client is None
        self.secret_keys: list[str] = secret_keys or []
        # Initialised in startup() because asyncio primitives need a running loop.
        self._sem: asyncio.Semaphore | None = None
        self._pending_tasks: set[asyncio.Task] = set()
        self._prune_task: asyncio.Task | None = None

    async def startup(self) -> None:
        if self._owns_client:
            self._client = httpx.AsyncClient()
        self._sem = asyncio.Semaphore(_DISPATCH_CONCURRENCY)
        self._prune_task = asyncio.create_task(
            _prune_old_deliveries(
                self._session_factory,
                _DELIVERY_PRUNE_INTERVAL,
                _DELIVERY_MAX_AGE_DAYS,
            )
        )

    async def shutdown(self) -> None:
        if self._prune_task is not None:
            self._prune_task.cancel()
            self._prune_task = None
        # Drain in-flight delivery tasks so we don't drop deliveries on shutdown.
        if self._pending_tasks:
            await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def dispatch(self, event: WebhookEvent, node_id: int) -> None:
        if self._client is None:
            return

        event_payload = event.model_dump(mode="json")

        async with self._session_factory() as db:
            # Find webhooks registered on any ancestor of (or equal to) node_id.
            # This allows a webhook on "/" (node_id=0) to catch everything.
            stmt = (
                select(orm.Webhook)
                .join(
                    orm.NodesClosure,
                    orm.NodesClosure.ancestor == orm.Webhook.node_id,
                )
                .where(orm.NodesClosure.descendant == node_id)
                .where(orm.Webhook.active.is_(True))
            )
            webhooks = (await db.execute(stmt)).scalars().all()

            rows_to_insert = []
            for wh in webhooks:
                # Per-webhook event filter
                events_filter = wh.events or []
                if events_filter and event.type not in events_filter:
                    continue

                event_id = uuid.uuid4().hex
                delivery = orm.WebhookDelivery(
                    webhook_id=wh.id,
                    event_id=event_id,
                    event_type=event.type,
                    payload=event_payload,
                    outcome=DeliveryOutcome.pending,
                    attempts=0,
                )
                db.add(delivery)
                rows_to_insert.append((wh, delivery, event_id))

            if rows_to_insert:
                await db.flush()  # assign delivery.id before leaving session

            # Decrypt secrets before leaving the session block; store
            # (webhook, delivery_id, plaintext_secret) for the tasks below.
            deliveries_to_fire = []
            for wh, delivery, event_id in rows_to_insert:
                plaintext_secret: Optional[str] = None
                if wh.secret:
                    plaintext_secret = _decrypt_secret(wh.secret, self.secret_keys)
                deliveries_to_fire.append((wh, delivery.id, plaintext_secret, event_id))
            await db.commit()

        # Fire background tasks outside the session, bounded by the semaphore.
        n_pending = len(self._pending_tasks)
        if n_pending >= _PENDING_TASK_WARN_THRESHOLD:
            logger.warning(
                "%d webhook delivery tasks outstanding; deliveries may be delayed. "
                "Consider reducing the number of registered webhooks or increasing "
                "the capacity of the egress infrastructure.",
                n_pending,
            )
        for wh, delivery_id, plaintext_secret, event_id in deliveries_to_fire:

            async def _guarded(
                _delivery_id=delivery_id,
                _url=wh.url,
                _secret=plaintext_secret,
                _event_id=event_id,
            ):
                async with self._sem:
                    await _deliver(
                        client=self._client,
                        session_factory=self._session_factory,
                        delivery_id=_delivery_id,
                        url=_url,
                        secret=_secret,
                        event_id=_event_id,
                        payload=event_payload,
                    )

            task = asyncio.create_task(_guarded())
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)
