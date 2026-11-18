import re
from pathlib import Path

import anyio
import pytest
from httpx import ASGITransport, AsyncClient
from starlette.status import HTTP_200_OK, HTTP_404_NOT_FOUND

from tiled.server.app import UI_BASE_TAG, build_app

FRONTEND_INDEX_HTML = Path(__file__).parents[1] / "web-frontend" / "index.html"

TILED_INDEX_HTML = (
    '<html><head><base href="/ui/" /></head>'
    '<body><script src="./assets/app.js"></script></body></html>'
)
VENDORED_INDEX_HTML = (
    '<html><head></head><body><script src="./assets/app.js"></script></body></html>'
)


@pytest.fixture
def serve_ui(tmp_path, monkeypatch):
    """Serve `index_html` as the UI distribution and report what a browser gets."""
    (tmp_path / "ui").mkdir()
    (tmp_path / "templates").mkdir()
    (tmp_path / "static").mkdir()
    (tmp_path / "static" / "default_ui_settings.yml").write_text(
        "api_url: /api/v1\nspecs: []\nstructure_families: {}\n"
    )
    monkeypatch.setattr("tiled.server.app.SHARE_TILED_PATH", tmp_path)
    monkeypatch.delenv("TILED_UI_SETTINGS", raising=False)

    async def serve(
        index_html=TILED_INDEX_HTML,
        *,
        root_path="",
        path="/ui/browse/deep/path",
    ):
        (tmp_path / "ui" / "index.html").write_text(index_html)
        app = build_app({})
        transport = ASGITransport(app=app, root_path=root_path)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            index = await client.get(path)
            settings = await client.get("/tiled-ui-settings")
        assert index.status_code == HTTP_200_OK
        assert settings.status_code == HTTP_200_OK
        return index.text, settings.json()

    return serve


def base_href(html):
    (href,) = re.findall(r'<base href="([^"]*)"', html)
    return href


@pytest.mark.parametrize(
    "root_path,expected",
    [
        pytest.param("", "", id="unmounted"),
        pytest.param("/", "", id="trailing-slash-stripped"),
        pytest.param("/tenant/ui/tiled", "/tenant/ui/tiled", id="mounted"),
    ],
)
@pytest.mark.asyncio
async def test_ui_uses_runtime_root_path(serve_ui, root_path, expected):
    html, settings = await serve_ui(root_path=root_path)

    assert base_href(html) == f"{expected}/ui/"
    assert settings["api_url"] == f"{expected}/api/v1"
    # Asset URLs stay relative, to be resolved against <base>.
    assert 'src="./assets/app.js"' in html


@pytest.mark.parametrize(
    "index_html,warns",
    [
        pytest.param(TILED_INDEX_HTML, False, id="tiled"),
        pytest.param(VENDORED_INDEX_HTML, True, id="vendored"),
    ],
)
@pytest.mark.asyncio
async def test_warns_when_base_tag_missing(serve_ui, caplog, index_html, warns):
    await serve_ui(index_html)

    assert (UI_BASE_TAG in caplog.text) == warns


@pytest.mark.asyncio
async def test_vendored_ui_is_served_verbatim(serve_ui):
    html, _ = await serve_ui(VENDORED_INDEX_HTML, root_path="/tiled", path="/ui/")

    assert html == VENDORED_INDEX_HTML


def test_frontend_declares_base_tag_the_server_rewrites():
    assert UI_BASE_TAG in FRONTEND_INDEX_HTML.read_text()


async def asgi_get(app, path):
    """GET `path` exactly as given.

    An HTTP client would normalize `..` and doubled slashes before sending.
    A server passes them through, after percent-decoding, as given here.
    """
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"test")],
        "client": ("127.0.0.1", 12345),
        "server": ("test", 80),
    }
    status = None
    body = b""
    request_sent = False

    async def receive():
        nonlocal request_sent
        if request_sent:
            # The client stays connected and sends nothing more.
            await anyio.sleep_forever()
        request_sent = True
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        nonlocal status, body
        if message["type"] == "http.response.start":
            status = message["status"]
        elif message["type"] == "http.response.body":
            body += message.get("body", b"")

    await app(scope, receive, send)
    return status, body


@pytest.fixture
def ui_app(tmp_path, monkeypatch):
    "An app serving a UI distribution, with a file next to it that is not part of it."
    share = tmp_path / "share"
    (share / "ui" / "assets").mkdir(parents=True)
    (share / "templates").mkdir()
    (share / "static").mkdir()
    (share / "static" / "default_ui_settings.yml").write_text(
        "api_url: /api/v1\nspecs: []\nstructure_families: {}\n"
    )
    monkeypatch.delenv("TILED_UI_SETTINGS", raising=False)
    (share / "ui" / "index.html").write_text(TILED_INDEX_HTML)
    (share / "ui" / "assets" / "app.js").write_text("// app")
    (share / "outside.txt").write_text("OUTSIDE")
    (tmp_path / "outside.txt").write_text("OUTSIDE")
    monkeypatch.setattr("tiled.server.app.SHARE_TILED_PATH", share)
    return build_app({}), tmp_path


@pytest.mark.asyncio
async def test_ui_serves_files_of_the_distribution(ui_app):
    app, _ = ui_app
    assert await asgi_get(app, "/ui/assets/app.js") == (HTTP_200_OK, b"// app")
    # A path with no file is left to the client-side application.
    status, body = await asgi_get(app, "/ui/browse/deep/path")
    assert status == HTTP_200_OK
    assert b"<html>" in body


@pytest.mark.parametrize(
    "path",
    [
        "/ui/../outside.txt",
        "/ui/assets/../../outside.txt",
        "/ui/../../outside.txt",
        "/ui/{tmp_path}/outside.txt",
        "/ui//{tmp_path}/outside.txt",
    ],
)
@pytest.mark.asyncio
async def test_ui_does_not_serve_files_outside_the_distribution(ui_app, path):
    app, tmp_path = ui_app
    status, body = await asgi_get(app, path.format(tmp_path=tmp_path))
    assert b"OUTSIDE" not in body
    assert status == HTTP_404_NOT_FOUND
