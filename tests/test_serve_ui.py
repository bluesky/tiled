import re
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.status import HTTP_200_OK

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
