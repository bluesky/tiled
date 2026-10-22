"""Tests for the client's custom httpx Transport, focusing on proxy handling.

Because Tiled supplies a custom transport to httpx.Client (to enable
client-side response caching), httpx's built-in resolution of the
HTTP_PROXY / HTTPS_PROXY / NO_PROXY environment variables is bypassed.
The Transport replicates that behavior so proxy env vars are honored.
"""
import httpx
import pytest

from tiled.client.transport import Transport


@pytest.fixture
def clear_proxy_env(monkeypatch):
    for var in (
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    ):
        monkeypatch.delenv(var, raising=False)


def test_no_proxy_env_means_no_mounts(clear_proxy_env):
    transport = Transport()
    assert transport._mounts == {}


def test_https_proxy_env_is_honored(clear_proxy_env, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9999")
    transport = Transport()
    # A request to an https URL should route through the proxy transport,
    # not the direct transport.
    selected = transport._transport_for_url(httpx.URL("https://example.com/path"))
    assert selected is not transport.transport
    assert selected._pool._proxy_url.host == b"127.0.0.1"
    assert selected._pool._proxy_url.port == 9999
    # A request to an http URL (no HTTP_PROXY set) should go direct.
    selected_http = transport._transport_for_url(httpx.URL("http://example.com/path"))
    assert selected_http is transport.transport


def test_no_proxy_bypass_is_honored(clear_proxy_env, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9999")
    monkeypatch.setenv("NO_PROXY", "internal.example.com")
    transport = Transport()
    # Host on the NO_PROXY list connects directly.
    direct = transport._transport_for_url(
        httpx.URL("https://internal.example.com/path")
    )
    assert direct is transport.transport
    # Other hosts still go through the proxy.
    proxied = transport._transport_for_url(httpx.URL("https://external.example.com/"))
    assert proxied is not transport.transport


def test_trust_env_false_ignores_proxy_env(clear_proxy_env, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9999")
    transport = Transport(trust_env=False)
    assert transport._mounts == {}
    selected = transport._transport_for_url(httpx.URL("https://example.com/"))
    assert selected is transport.transport


def test_explicit_transport_skips_proxy_handling(clear_proxy_env, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9999")
    inner = httpx.HTTPTransport()
    transport = Transport(transport=inner)
    # When an explicit transport is provided (e.g. ASGI), no proxy mounts.
    assert transport._mounts == {}
    assert transport.transport is inner


def test_verify_is_honored_on_inner_transport(clear_proxy_env):
    transport = Transport(verify=False)
    import ssl

    assert transport.transport._pool._ssl_context.verify_mode == ssl.CERT_NONE
