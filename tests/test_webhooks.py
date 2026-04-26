"""Tests for the DB-backed webhook dispatcher."""

import asyncio
import hashlib
import hmac
import json
import socket
from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import quote as urlquote

import anyio
import httpx
import pytest
import respx
import stamina
from fastapi import FastAPI
from httpx import Response
from sqlalchemy import select as sa_select

from tiled.catalog import from_uri, in_memory
from tiled.catalog.adapter import CatalogNodeAdapter
from tiled.catalog.orm import Webhook
from tiled.client import Context, from_context
from tiled.client.container import Container
from tiled.client.context import password_grant
from tiled.config import Authentication, WebhooksConfig
from tiled.server.app import build_app
from tiled.server.schemas import (
    DeliveryResponse,
    EventType,
    StreamClosedEvent,
    WebhookRegistrationRequest,
    WebhookResponse,
)
from tiled.server.webhooks import (
    MAX_ATTEMPTS,
    WebhookDispatcher,
    _decrypt_secret,
    _deliver,
    _encrypt_secret,
    _sign,
    check_url_ssrf_safety,
)

from .conftest import TOY_AUTHENTICATION
from .utils import enter_username_password

WEBHOOK_URL = "https://webhook.example.com/tiled-events"


def _wh_req(**kwargs: Any) -> dict[str, Any]:
    """Build a webhook registration request payload for WEBHOOK_URL."""
    return WebhookRegistrationRequest(url=WEBHOOK_URL, **kwargs).model_dump(mode="json")


def _register_webhook(
    http: httpx.Client, path: str = "", **kwargs: Any
) -> WebhookResponse:
    """Register a webhook on ``path`` (default: root) and return the validated response."""
    encoded = urlquote(path, safe="/")
    return WebhookResponse.model_validate(
        http.post(f"/api/v1/webhooks/target/{encoded}", json=_wh_req(**kwargs))
        .raise_for_status()
        .json()
    )


def _capturing_mock() -> list[dict[str, Any]]:
    """Attach a respx side-effect that captures decoded payloads and returns 200."""
    received: list[dict[str, Any]] = []

    def _capture(request: httpx.Request) -> Response:
        received.append(json.loads(request.content))
        return Response(200)

    respx.post(WEBHOOK_URL).mock(side_effect=_capture)
    return received


@pytest.fixture(scope="module")
def app(tmpdir_module: Any) -> FastAPI:
    tree = in_memory(
        writable_storage=[f"file://localhost{tmpdir_module / 'data'}"],
    )
    return build_app(
        tree,
        authentication=Authentication(
            providers=TOY_AUTHENTICATION["providers"],
            secret_keys=TOY_AUTHENTICATION["secret_keys"],
            tiled_admins=[{"provider": "toy", "id": "alice"}],
        ),
        server_settings={"webhooks": WebhooksConfig(secret_keys=["test-webhook-key"])},
    )


@pytest.fixture(scope="module")
def context(app: FastAPI) -> Generator[Context, None, None]:
    with Context.from_app(app) as ctx:
        with enter_username_password("alice", "secret1"):
            from_context(ctx)  # triggers login, caches tokens
        yield ctx


@pytest.fixture(scope="module")
def http(context: Context) -> httpx.Client:
    return context.http_client


@pytest.fixture(scope="module")
def client(context: Context) -> Container:
    return from_context(context)


@pytest.fixture(scope="module")
def bob_http(context: Context) -> httpx.Client:
    """Authenticated HTTP client for bob (non-admin user)."""
    provider = context.server_info.authentication.providers[0]
    auth_endpoint = provider.links["auth_endpoint"]
    tokens = password_grant(
        context.http_client, auth_endpoint, provider.provider, "bob", "secret2"
    )
    access_token = tokens["access_token"]
    return httpx.Client(
        transport=context.http_client._transport,
        base_url=str(context.http_client.base_url),
        headers={"Authorization": f"Bearer {access_token}"},
        follow_redirects=True,
    )


@pytest.fixture
def mock_delivery_session() -> tuple[MagicMock, MagicMock]:
    """Return a (delivery, session_factory) pair backed by AsyncMock."""
    delivery = MagicMock()
    delivery.id = 1
    db = AsyncMock()
    db.get = AsyncMock(return_value=delivery)
    db.__aenter__ = AsyncMock(return_value=db)
    db.__aexit__ = AsyncMock(return_value=False)
    session_factory = MagicMock(return_value=db)
    return delivery, session_factory


@pytest.fixture
def enable_retries() -> Generator[None, None, None]:
    """Re-enable stamina retries for tests that specifically test retry behavior.

    The session-scoped ``deactivate_retries`` autouse fixture in conftest.py
    globally sets ``stamina.set_active(False)``.  Tests that exercise retry
    logic must opt back in with this fixture.
    """
    stamina.set_active(True)
    yield
    stamina.set_active(False)


# ---------------------------------------------------------------------------
# Unit tests: _sign / encrypt helpers
# ---------------------------------------------------------------------------


def test_sign_produces_hmac_sha256() -> None:
    body = b'{"type": "test"}'
    secret = "mysecret"
    result = _sign(body, secret)
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert result == expected


def test_encrypt_decrypt_roundtrip() -> None:
    secret_keys = ["key1", "key2"]
    plaintext = "my-hmac-signing-secret"
    encrypted = _encrypt_secret(plaintext, secret_keys)
    # Must not store plaintext
    assert plaintext not in encrypted
    # Must round-trip correctly
    assert _decrypt_secret(encrypted, secret_keys) == plaintext


def test_decrypt_with_rotated_key() -> None:
    """Secret encrypted with old key can be decrypted when that key is still
    present in the list (key rotation support)."""
    old_keys = ["old-key"]
    new_keys = ["new-key", "old-key"]
    plaintext = "rotation-test-secret"
    encrypted = _encrypt_secret(plaintext, old_keys)
    assert _decrypt_secret(encrypted, new_keys) == plaintext


def test_decrypt_wrong_key_returns_none(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("CRITICAL"):  # suppress expected ERROR log
        encrypted = _encrypt_secret("secret", ["correct-key"])
        result = _decrypt_secret(encrypted, ["wrong-key"])
    assert result is None


# ---------------------------------------------------------------------------
# Unit tests: _deliver retry logic
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deliver_success_updates_delivery_row(
    mock_delivery_session: tuple[MagicMock, MagicMock],
) -> None:
    delivery, session_factory = mock_delivery_session
    payload: dict[str, Any] = {"type": "container-child-created"}

    with respx.mock:
        respx.post(WEBHOOK_URL).mock(return_value=Response(200))
        async with httpx.AsyncClient() as c:
            await _deliver(
                client=c,
                session_factory=session_factory,
                delivery_id=1,
                url=WEBHOOK_URL,
                secret=None,
                event_id="test-event-id",
                payload=payload,
            )

    assert delivery.outcome == "success"
    assert delivery.status_code == 200


@pytest.mark.asyncio
async def test_deliver_retries_on_failure(
    mock_delivery_session: tuple[MagicMock, MagicMock],
    enable_retries: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    delivery, session_factory = mock_delivery_session
    payload: dict[str, Any] = {"type": "container-child-created"}

    with caplog.at_level("CRITICAL"):  # suppress expected retry WARNING/ERROR logs
        with respx.mock:
            respx.post(WEBHOOK_URL).mock(return_value=Response(500))
            with stamina.set_testing(True, attempts=MAX_ATTEMPTS):
                async with httpx.AsyncClient() as c:
                    await _deliver(
                        client=c,
                        session_factory=session_factory,
                        delivery_id=1,
                        url=WEBHOOK_URL,
                        secret=None,
                        event_id="test-event-id",
                        payload=payload,
                    )

    assert delivery.outcome == "failed"
    assert delivery.attempts == MAX_ATTEMPTS


# ---------------------------------------------------------------------------
# Integration tests
#
# ---------------------------------------------------------------------------


class TestWebhookIntegration:
    @pytest.fixture(autouse=True)
    def bypass_ssrf_check(self) -> Generator[None, None, None]:
        """Disable the SSRF blocklist for integration tests."""
        with patch("tiled.server.webhook_router.check_url_ssrf_safety"):
            yield

    @pytest.fixture(autouse=True)
    def cleanup_webhooks(self, http: httpx.Client) -> Generator[list[int], None, None]:
        """Yield a collector list; delete every webhook ID added to it after the test."""
        extra_ids: list[int] = []
        yield extra_ids
        # Auto-clean root-level webhooks.
        root_ids = [wh["id"] for wh in http.get("/api/v1/webhooks/target/").json()]
        for wh_id in set(root_ids + extra_ids):
            http.delete(f"/api/v1/webhooks/{wh_id}")

    @pytest.fixture
    def node_key(self, request: pytest.FixtureRequest) -> str:
        """Return a container key unique within the module (derived from test name)."""
        return request.node.name.replace("[", "_").replace("]", "")

    @pytest.fixture
    def registered_webhook(self, http: httpx.Client) -> int:
        """Register a catch-all webhook on the root node and return its id."""
        return _register_webhook(http).id

    @respx.mock
    def test_register_webhook_and_fires_on_create_container(
        self, http: httpx.Client, client: Container, node_key: str
    ) -> None:
        received = _capturing_mock()

        _register_webhook(http, events=[EventType.container_child_created])
        client.create_container(node_key, metadata={"material": "NiPS3"})

        assert len(received) == 1
        event = received[0]
        assert event["type"] == EventType.container_child_created
        assert event["key"] == node_key

    @respx.mock
    def test_register_webhook_returns_webhook_in_list(self, http: httpx.Client) -> None:
        respx.post(WEBHOOK_URL).mock(return_value=Response(200))

        http.post(
            "/api/v1/webhooks/target/",
            json=_wh_req(),
        ).raise_for_status()

        webhooks = [
            WebhookResponse.model_validate(w)
            for w in http.get("/api/v1/webhooks/target/").raise_for_status().json()
        ]

        assert len(webhooks) == 1
        assert webhooks[0].url.unicode_string() == WEBHOOK_URL

    @respx.mock
    def test_delete_webhook_stops_delivery(
        self, http: httpx.Client, client: Container, node_key: str
    ) -> None:
        route = respx.post(WEBHOOK_URL).mock(return_value=Response(200))
        wh = _register_webhook(http)
        http.delete(f"/api/v1/webhooks/{wh.id}").raise_for_status()
        client.create_container(node_key)
        assert not route.called

    @respx.mock
    def test_dispatcher_hmac_signature(
        self, http: httpx.Client, client: Container, node_key: str
    ) -> None:
        secret = "mysecret"
        received_headers: dict[str, str] = {}

        def capture(request: httpx.Request) -> Response:
            received_headers.update(dict(request.headers))
            return Response(200)

        respx.post(WEBHOOK_URL).mock(side_effect=capture)
        _register_webhook(http, secret=secret)
        client.create_container(node_key)

        sig = received_headers.get("x-tiled-signature", "")
        assert sig.startswith("sha256=")

    @respx.mock
    def test_secret_stored_encrypted_not_plaintext(
        self, app: FastAPI, http: httpx.Client
    ) -> None:
        secret = "super-secret-value"
        respx.post(WEBHOOK_URL).mock(return_value=Response(200))
        _register_webhook(http, secret=secret)

        # Directly query the DB to inspect the stored value.
        # Use anyio.from_thread.start_blocking_portal() to safely run async
        # code from a sync test context regardless of whether an event loop
        # is already running in the current thread (e.g. under the httpx
        # ASGI transport).
        catalog_context = app.state.root_tree.context

        async def _get_stored_secret() -> str:
            async with catalog_context.session() as db:
                row = (await db.execute(sa_select(Webhook))).scalars().first()
                return row.secret

        with anyio.from_thread.start_blocking_portal() as portal:
            stored: str = portal.call(_get_stored_secret)

        assert stored is not None
        # Plaintext must not appear in the stored value
        assert secret not in stored
        # Fernet tokens are base64 and start with 'g' (version byte 0x80)
        assert stored.startswith("g")

    @respx.mock
    def test_webhook_fires_for_descendant_node(
        self, http: httpx.Client, client: Container, node_key: str
    ) -> None:
        received = _capturing_mock()
        _register_webhook(http)

        dataset = client.create_container(node_key)
        dataset.create_container("child")

        assert len(received) == 2
        paths = [e["path"] for e in received]
        assert [node_key] in paths
        assert [node_key, "child"] in paths

    @respx.mock
    def test_delivery_history_endpoint(
        self,
        http: httpx.Client,
        client: Container,
        registered_webhook: int,
        node_key: str,
    ) -> None:
        respx.post(WEBHOOK_URL).mock(return_value=Response(200))

        client.create_container(node_key)

        history = [
            DeliveryResponse.model_validate(d)
            for d in http.get(f"/api/v1/webhooks/history/{registered_webhook}")
            .raise_for_status()
            .json()
        ]

        assert len(history) >= 1
        assert history[0].webhook_id == registered_webhook

    @respx.mock
    def test_subnode_webhook_fires_for_descendant_only(
        self,
        http: httpx.Client,
        client: Container,
        node_key: str,
        cleanup_webhooks: list[int],
    ) -> None:
        """Webhook on a sub-node fires for events at or below that node,
        but NOT for events on a sibling sub-node."""
        dataset = client.create_container(node_key)
        sibling_key = f"{node_key}_sibling"
        sibling = client.create_container(sibling_key)

        received = _capturing_mock()

        # Register webhook on node_key only (not root, not sibling).
        wh = _register_webhook(http, path=node_key)
        cleanup_webhooks.append(wh.id)  # not visible to root list query

        # Create a child under the watched node → should fire.
        dataset.create_container("watched_child")
        # Create a child under the sibling → must NOT fire.
        sibling.create_container("unwatched_child")

        paths = [e["path"] for e in received]
        assert any(node_key in p for p in paths), "expected event for watched sub-tree"
        assert not any(
            sibling_key in str(p) for p in paths
        ), "unexpected event for sibling sub-tree"

    @respx.mock
    def test_subnode_webhook_in_list_for_its_path(
        self,
        http: httpx.Client,
        client: Container,
        node_key: str,
        cleanup_webhooks: list[int],
    ) -> None:
        """A webhook registered on a sub-node is returned by GET /webhooks/target/{node}
        (webhooks on that node) but absent from GET /webhooks/target/ (webhooks on root).
        """
        respx.post(WEBHOOK_URL).mock(return_value=Response(200))

        client.create_container(node_key)
        wh = _register_webhook(http, path=node_key)
        cleanup_webhooks.append(wh.id)  # not visible to root list query

        encoded = urlquote(node_key, safe="/")
        sub_webhooks = [
            WebhookResponse.model_validate(w)
            for w in http.get(f"/api/v1/webhooks/target/{encoded}")
            .raise_for_status()
            .json()
        ]
        assert any(w.id == wh.id for w in sub_webhooks)

        root_webhooks = [
            WebhookResponse.model_validate(w)
            for w in http.get("/api/v1/webhooks/target/").raise_for_status().json()
        ]
        assert not any(w.id == wh.id for w in root_webhooks)

    @respx.mock
    def test_metadata_updated_fires_webhook(
        self, http: httpx.Client, client: Container, node_key: str
    ) -> None:
        received = _capturing_mock()
        _register_webhook(http, events=[EventType.container_child_metadata_updated])

        node = client.create_container(node_key, metadata={"color": "blue"})
        node.update_metadata(metadata={"color": "red"})

        assert len(received) == 1
        event = received[0]
        assert event["type"] == EventType.container_child_metadata_updated
        assert event["key"] == node_key

    @respx.mock
    def test_event_filter_excludes_wrong_type(
        self, http: httpx.Client, client: Container, node_key: str
    ) -> None:
        """A webhook registered for container_child_created must NOT fire when
        container_child_metadata_updated is dispatched."""
        received = _capturing_mock()
        _register_webhook(http, events=[EventType.container_child_created])

        node = client.create_container(node_key, metadata={"x": 1})
        received.clear()  # discard the create event itself
        node.update_metadata(metadata={"x": 2})

        assert received == [], (
            "Webhook registered for container_child_created fired on "
            "container_child_metadata_updated"
        )

    @respx.mock
    def test_metadata_payload_correct_when_only_specs_updated(
        self, http: httpx.Client, client: Container, node_key: str
    ) -> None:
        """When replace_metadata is called with specs only (no metadata argument),
        the webhook payload must carry the pre-existing metadata, not an empty dict."""
        received = _capturing_mock()
        _register_webhook(http, events=[EventType.container_child_metadata_updated])

        node = client.create_container(node_key, metadata={"color": "blue"})
        received.clear()  # discard the create event

        # Update only the specs, leave metadata unchanged.
        node.update_metadata(specs=[])

        assert len(received) == 1, "Expected exactly one metadata-updated webhook"
        event = received[0]
        assert event["type"] == EventType.container_child_metadata_updated
        # The payload must reflect the existing metadata, not an empty dict.
        assert event.get("metadata") == {
            "color": "blue"
        }, "Webhook payload should include pre-existing metadata when only specs changed"

    @respx.mock
    def test_webhook_cascade_deleted_when_node_deleted(
        self,
        http: httpx.Client,
        client: Container,
        node_key: str,
    ) -> None:
        """Deleting a node must cascade-delete its webhooks so that subsequent
        events on other nodes do NOT trigger the now-deleted webhook."""
        received = _capturing_mock()

        # Create a sub-node and register a webhook on it.
        client.create_container(node_key)
        _register_webhook(
            http, path=node_key, events=[EventType.container_child_created]
        )

        # Delete the sub-node (cascades the webhook via FK ON DELETE CASCADE).
        client.delete_contents(keys=[node_key], recursive=True, external_only=False)

        # Create an unrelated container at root — must NOT trigger the deleted webhook.
        sibling_key = f"{node_key}_after_delete"
        client.create_container(sibling_key)

        assert received == [], (
            "Webhook registered on a deleted node still fired after the node "
            "was removed (CASCADE delete failed)"
        )

    @respx.mock
    def test_close_stream_fires_webhook(
        self, http: httpx.Client, client: Container, node_key: str
    ) -> None:
        """Calling close_stream on a node must dispatch a stream-closed webhook event."""
        received = _capturing_mock()

        client.create_container(node_key)
        _register_webhook(http, events=[EventType.stream_closed])

        # Hit the close-stream endpoint directly.
        http.delete(f"/api/v1/stream/close/{node_key}").raise_for_status()

        assert len(received) == 1
        event = received[0]
        assert event["type"] == EventType.stream_closed
        assert event["key"] == node_key

    # -----------------------------------------------------------------------
    # Scope enforcement: non-admin users must be rejected
    # -----------------------------------------------------------------------

    @pytest.mark.parametrize(
        "method,path,kwargs",
        [
            ("post", "/api/v1/webhooks/target/", {"json": _wh_req()}),
            ("get", "/api/v1/webhooks/target/", {}),
            ("delete", "/api/v1/webhooks/999999", {}),
            ("get", "/api/v1/webhooks/history/999999", {}),
        ],
        ids=["register", "list", "delete", "history"],
    )
    def test_non_admin_rejected(
        self, bob_http: httpx.Client, method: str, path: str, kwargs: dict
    ) -> None:
        """Non-admin user must get 401 for all webhook endpoints."""
        resp = getattr(bob_http, method)(path, **kwargs)
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Integration test: SSRF endpoint check
# (must live outside TestWebhookIntegration, which bypasses check_url_ssrf_safety)
# ---------------------------------------------------------------------------


def test_ssrf_private_ip_rejected(http: httpx.Client) -> None:
    original_getaddrinfo = socket.getaddrinfo

    def _fake_getaddrinfo(host, port, *args, **kwargs):
        if host == "internal.local":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 0))]
        return original_getaddrinfo(host, port, *args, **kwargs)

    with patch(
        "tiled.server.webhooks.socket.getaddrinfo", side_effect=_fake_getaddrinfo
    ):
        resp = http.post(
            "/api/v1/webhooks/target/",
            json={"url": "https://internal.local/hook"},
        )
    assert resp.status_code == 400
    assert "private" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Unit tests: SSRF blocklist
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url,blocked_ip",
    [
        ("https://localhost/hook", "127.0.0.1"),
        ("https://internal/hook", "10.0.0.1"),
        ("https://dev/hook", "172.16.5.5"),
        ("https://local/hook", "192.168.1.1"),
        ("https://meta/hook", "169.254.169.254"),  # cloud metadata
    ],
)
def test_ssrf_check_blocks_private_ips(url: str, blocked_ip: str) -> None:
    original = socket.getaddrinfo

    def _fake(host, port, *args, **kwargs):
        parsed_host = url.split("//")[1].split("/")[0]
        if host == parsed_host:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (blocked_ip, 0))]
        return original(host, port, *args, **kwargs)

    with patch("tiled.server.webhooks.socket.getaddrinfo", side_effect=_fake):
        with pytest.raises(ValueError, match="blocked"):
            check_url_ssrf_safety(url)


def test_ssrf_check_allows_public_ip() -> None:
    """check_url_ssrf_safety does not raise for a public IP."""

    def _fake(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]

    with patch("tiled.server.webhooks.socket.getaddrinfo", side_effect=_fake):
        check_url_ssrf_safety("https://example.com/hook")  # must not raise


# ---------------------------------------------------------------------------
# Unit tests: WebhookDispatcher shutdown drains pending tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatcher_shutdown_waits_for_pending_tasks() -> None:
    completed: list[str] = []

    async def _slow_deliver() -> None:
        await asyncio.sleep(0.05)
        completed.append("done")

    dispatcher = WebhookDispatcher(session_factory=AsyncMock(), _client=AsyncMock())
    await dispatcher.startup()

    # Inject a slow task directly into _pending_tasks.
    task = asyncio.create_task(_slow_deliver())
    dispatcher._pending_tasks.add(task)
    task.add_done_callback(dispatcher._pending_tasks.discard)

    await dispatcher.shutdown()

    assert completed == ["done"], "shutdown() returned before pending task completed"


# ---------------------------------------------------------------------------
# Unit tests: close_stream webhook dispatch
# ---------------------------------------------------------------------------


def _make_close_stream_adapter(
    *, webhook_dispatcher, streaming_cache, node_id=42, node_key="stream"
) -> CatalogNodeAdapter:
    """Build a CatalogNodeAdapter suitable for testing close_stream()."""
    mock_context = MagicMock()
    mock_context.webhook_dispatcher = webhook_dispatcher
    mock_context.streaming_cache = streaming_cache

    mock_node = MagicMock()
    mock_node.id = node_id
    mock_node.key = node_key
    mock_node.structure_family = "container"
    mock_node.specs = []
    mock_node.access_blob = {}

    adapter = CatalogNodeAdapter(mock_context, mock_node)
    adapter.path_segments = AsyncMock(return_value=[node_key])
    return adapter


@pytest.mark.asyncio
async def test_close_stream_dispatches_webhook() -> None:
    dispatched: list[StreamClosedEvent] = []

    mock_dispatcher = MagicMock()
    mock_dispatcher.dispatch = AsyncMock(
        side_effect=lambda event, **kw: dispatched.append(event) or None
    )

    mock_streaming_cache = MagicMock()
    mock_streaming_cache.close = AsyncMock()

    adapter = _make_close_stream_adapter(
        webhook_dispatcher=mock_dispatcher,
        streaming_cache=mock_streaming_cache,
        node_key="my_stream",
    )

    await adapter.close_stream()

    assert len(dispatched) == 1
    event = dispatched[0]
    assert event.type == EventType.stream_closed
    assert event.key == "my_stream"


# ---------------------------------------------------------------------------
# Unit tests: close_stream guards (no dispatcher, no streaming cache)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_stream_no_webhook_dispatcher() -> None:
    """close_stream() must not raise when webhook_dispatcher is None (webhooks disabled)."""
    mock_streaming_cache = MagicMock()
    mock_streaming_cache.close = AsyncMock()

    adapter = _make_close_stream_adapter(
        webhook_dispatcher=None,
        streaming_cache=mock_streaming_cache,
    )

    await adapter.close_stream()  # must not raise


@pytest.mark.asyncio
async def test_close_stream_no_streaming_cache() -> None:
    """close_stream() must not raise when streaming_cache is None (no Redis)."""
    mock_dispatcher = MagicMock()
    mock_dispatcher.dispatch = AsyncMock()

    adapter = _make_close_stream_adapter(
        webhook_dispatcher=mock_dispatcher,
        streaming_cache=None,
    )

    await adapter.close_stream()  # must not raise


# ---------------------------------------------------------------------------
# Unit tests: _deliver additional paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deliver_network_error_sets_outcome_failed(
    mock_delivery_session: tuple[MagicMock, MagicMock],
) -> None:
    """A network-level exception (not HTTP) must set outcome=failed with status_code=None."""
    delivery, session_factory = mock_delivery_session
    payload: dict[str, Any] = {"type": "container-child-created"}

    with respx.mock:
        respx.post(WEBHOOK_URL).mock(side_effect=httpx.ConnectError("refused"))
        async with httpx.AsyncClient() as c:
            await _deliver(
                client=c,
                session_factory=session_factory,
                delivery_id=1,
                url=WEBHOOK_URL,
                secret=None,
                event_id="test-event-id",
                payload=payload,
            )

    assert delivery.outcome == "failed"
    assert delivery.status_code is None
    assert delivery.error_detail is not None


@pytest.mark.asyncio
async def test_deliver_deleted_row_does_not_raise(
    mock_delivery_session: tuple[MagicMock, MagicMock],
) -> None:
    """If the delivery row is deleted between task creation and the DB update,
    _deliver() must not raise (the row-not-found guard handles it silently)."""
    delivery, session_factory = mock_delivery_session
    # Make db.get() return None — simulates the row being deleted mid-flight.
    session_factory.return_value.__aenter__.return_value.get = AsyncMock(
        return_value=None
    )

    payload: dict[str, Any] = {"type": "container-child-created"}

    with respx.mock:
        respx.post(WEBHOOK_URL).mock(return_value=Response(200))
        async with httpx.AsyncClient() as c:
            await _deliver(
                client=c,
                session_factory=session_factory,
                delivery_id=1,
                url=WEBHOOK_URL,
                secret=None,
                event_id="test-event-id",
                payload=payload,
            )  # must not raise


@pytest.mark.asyncio
async def test_deliver_sends_event_id_header(
    mock_delivery_session: tuple[MagicMock, MagicMock],
) -> None:
    """The X-Tiled-Event-ID header must be present in every outgoing request."""
    delivery, session_factory = mock_delivery_session
    sent_headers: list[dict] = []

    def _capture(request: httpx.Request) -> Response:
        sent_headers.append(dict(request.headers))
        return Response(200)

    payload: dict[str, Any] = {"type": "container-child-created"}

    with respx.mock:
        respx.post(WEBHOOK_URL).mock(side_effect=_capture)
        async with httpx.AsyncClient() as c:
            await _deliver(
                client=c,
                session_factory=session_factory,
                delivery_id=1,
                url=WEBHOOK_URL,
                secret=None,
                event_id="my-unique-event-id",
                payload=payload,
            )

    assert sent_headers, "No request was captured"
    assert sent_headers[0].get("x-tiled-event-id") == "my-unique-event-id"


@pytest.mark.asyncio
async def test_deliver_no_signature_when_no_secret(
    mock_delivery_session: tuple[MagicMock, MagicMock],
) -> None:
    """X-Tiled-Signature must be absent when no secret is configured."""
    delivery, session_factory = mock_delivery_session
    sent_headers: list[dict] = []

    def _capture(request: httpx.Request) -> Response:
        sent_headers.append(dict(request.headers))
        return Response(200)

    payload: dict[str, Any] = {"type": "container-child-created"}

    with respx.mock:
        respx.post(WEBHOOK_URL).mock(side_effect=_capture)
        async with httpx.AsyncClient() as c:
            await _deliver(
                client=c,
                session_factory=session_factory,
                delivery_id=1,
                url=WEBHOOK_URL,
                secret=None,
                event_id="test-event-id",
                payload=payload,
            )

    assert "x-tiled-signature" not in sent_headers[0]


# ---------------------------------------------------------------------------
# Unit tests: SSRF — unresolvable hostname
# ---------------------------------------------------------------------------


def test_ssrf_check_unresolvable_hostname() -> None:
    """An unresolvable hostname must raise ValueError before any network call."""
    with patch(
        "tiled.server.webhooks.socket.getaddrinfo",
        side_effect=socket.gaierror("Name or service not known"),
    ):
        with pytest.raises(ValueError, match="Cannot resolve"):
            check_url_ssrf_safety("https://does-not-exist.invalid/hook")


# ---------------------------------------------------------------------------
# Integration tests: webhooks-disabled path
# ---------------------------------------------------------------------------


def test_webhooks_disabled_router_not_mounted(tmp_path: Any) -> None:
    """When no webhooks: config section is provided, the /api/v1/webhooks router
    must not be mounted and all webhook endpoints must return 404."""
    tree = from_uri(
        f"sqlite+aiosqlite:///{tmp_path / 'disabled.db'}",
        writable_storage=[str(tmp_path / "data")],
        init_if_not_exists=True,
    )
    app_no_webhooks = build_app(
        tree,
        authentication=Authentication(single_user_api_key="secret"),
        # No server_settings with webhooks key
    )
    with Context.from_app(app_no_webhooks) as ctx:
        http = ctx.http_client
        resp = http.get("/api/v1/webhooks/target/")
        assert (
            resp.status_code == 404
        ), "Webhook router should not be mounted when webhooks: is absent from config"


# ---------------------------------------------------------------------------
# Integration tests: schema validators
# ---------------------------------------------------------------------------


def test_register_webhook_http_url_rejected(http: httpx.Client) -> None:
    """A plain HTTP URL must be rejected at schema validation (HTTP 422)."""
    resp = http.post(
        "/api/v1/webhooks/target/",
        json={"url": "http://example.com/hook"},
    )
    assert resp.status_code == 422


def test_register_webhook_empty_events_normalized_to_none() -> None:
    """An empty events list must be treated the same as omitting events (catch-all)."""
    req = WebhookRegistrationRequest(url=WEBHOOK_URL, events=[])
    assert req.events is None, "events=[] should be normalised to None (catch-all)"


# ---------------------------------------------------------------------------
# Integration tests: CRUD edge cases
# ---------------------------------------------------------------------------


def test_delete_nonexistent_webhook_returns_404(http: httpx.Client) -> None:
    """DELETE on a webhook ID that does not exist must return 404."""
    resp = http.delete("/api/v1/webhooks/999999")
    assert resp.status_code == 404


def test_history_nonexistent_webhook_returns_404(http: httpx.Client) -> None:
    """GET /history/{id} for a non-existent webhook must return 404."""
    resp = http.get("/api/v1/webhooks/history/999999")
    assert resp.status_code == 404
