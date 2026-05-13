"""WebDAV endpoint for Tiled (/api/webdav).

Exposes the Tiled tree as a WebDAV filesystem:
- Containers become WebDAV collections (directories).
- Arrays and tables become collections whose children are the backing data files.
- GET on an array/table returns a ZIP of those backing files.
- GET on a child path (e.g. /api/webdav/ds/file.tif) returns the raw file.

Only file:// assets are served; other schemes (s3://, etc.) are skipped.

Authentication: WebDAV clients use HTTP Basic Auth. The username is ignored;
the password is treated as a Tiled API key.  WebDAVBasicAuthMiddleware rewrites
the Authorization header before the normal auth chain runs.
"""

import base64
import io
import os
import zipfile
from typing import Optional, Union
from urllib.parse import urlparse
from xml.etree.ElementTree import Element, SubElement, tostring

import pydantic_settings
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import FileResponse
from starlette.responses import Response
from starlette.status import HTTP_404_NOT_FOUND, HTTP_405_METHOD_NOT_ALLOWED

from ..structures.core import StructureFamily
from ..type_aliases import AccessTags, Scopes
from ..utils import path_from_uri
from .authentication import (
    check_scopes,
    get_current_access_tags,
    get_current_principal,
    get_current_scopes,
    get_session_state,
)
from .dependencies import get_entry, get_root_tree
from .schemas import Principal

DAV_NS = "DAV:"
TILED_WEBDAV_NS = "https://blueskiyproject.io/tiled"

_WEBDAV_PREFIX = "/api/webdav"


class WebDAVBasicAuthMiddleware:
    """Handle Basic Auth for /api/webdav requests.

    Two jobs:

    Inbound — rewrite incoming Basic credentials so the normal Tiled auth
    chain picks them up:
        Authorization: Basic base64(anything:apikey)
        → Authorization: Apikey apikey
    The username field is ignored; the password IS the Tiled API key.

    Outbound — when the downstream stack returns 401, replace the
    WWW-Authenticate header with a Basic challenge.  WebDAV clients
    don't understand Bearer and will never prompt the user otherwise.
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not scope.get("path", "").startswith(
            _WEBDAV_PREFIX
        ):
            await self.app(scope, receive, send)
            return

        # --- Inbound: Basic → Apikey ---
        headers = list(scope["headers"])
        for i, (name, value) in enumerate(headers):
            if name == b"authorization":
                auth = value.decode("latin-1")
                if auth[:6].lower() == "basic ":
                    try:
                        decoded = base64.b64decode(auth[6:]).decode(
                            "utf-8", errors="replace"
                        )
                        _, _, api_key = decoded.partition(":")
                        if api_key:
                            headers[i] = (
                                b"authorization",
                                f"Apikey {api_key}".encode("latin-1"),
                            )
                    except Exception:
                        pass
                break
        scope = {**scope, "headers": headers}

        # --- Outbound: replace WWW-Authenticate on 401 with Basic challenge ---
        async def webdav_send(message):
            if (
                message["type"] == "http.response.start"
                and message.get("status") == 401
            ):
                new_headers = [
                    (name, value)
                    for name, value in message.get("headers", [])
                    if name.lower() != b"www-authenticate"
                ]
                new_headers.append((b"www-authenticate", b'Basic realm="tiled"'))
                message = {**message, "headers": new_headers}
            await send(message)

        await self.app(scope, receive, webdav_send)


def _dav(tag: str) -> str:
    return f"{{{DAV_NS}}}{tag}"


def _tiled(tag: str) -> str:
    return f"{{{TILED_WEBDAV_NS}}}{tag}"


def _build_propfind_xml(resources: list[dict]) -> bytes:
    """Render a WebDAV 207 Multi-Status XML response body."""
    ms = Element(_dav("multistatus"))
    ms.set("xmlns:D", "DAV:")
    ms.set("xmlns:T", TILED_WEBDAV_NS)

    for res in resources:
        r = SubElement(ms, _dav("response"))

        href_el = SubElement(r, _dav("href"))
        href_el.text = res["href"]

        ps = SubElement(r, _dav("propstat"))
        prop = SubElement(ps, _dav("prop"))

        dn = SubElement(prop, _dav("displayname"))
        dn.text = res.get("displayname", "")

        rt = SubElement(prop, _dav("resourcetype"))
        if res.get("is_collection"):
            SubElement(rt, _dav("collection"))

        if "contentlength" in res:
            cl = SubElement(prop, _dav("getcontentlength"))
            cl.text = str(res["contentlength"])

        if "contenttype" in res:
            ct = SubElement(prop, _dav("getcontenttype"))
            ct.text = res["contenttype"]

        if "structure_family" in res:
            sf = SubElement(prop, _tiled("structure_family"))
            sf.text = res["structure_family"]

        st = SubElement(ps, _dav("status"))
        st.text = "HTTP/1.1 200 OK"

    return b'<?xml version="1.0" encoding="UTF-8"?>' + tostring(
        ms, encoding="unicode"
    ).encode("utf-8")


def _asset_filename(data_uri: str) -> str:
    return os.path.basename(urlparse(data_uri).path.rstrip("/"))


def _file_assets(entry) -> list:
    """Return file:// Asset objects from an entry's data_sources, or []."""
    if not hasattr(entry, "data_sources"):
        return []
    data_sources = entry.data_sources
    if callable(data_sources):
        data_sources = data_sources()
    result = []
    for ds in data_sources or []:
        for asset in ds.assets:
            if urlparse(asset.data_uri).scheme == "file":
                result.append(asset)
    return result


def _zip_assets(assets: list, zip_name: str) -> Response:
    """Build an in-memory ZIP of file assets and return it."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for asset in assets:
            asset_path = path_from_uri(asset.data_uri)
            if asset.is_directory:
                for root, _, files in os.walk(asset_path):
                    for fname in files:
                        full = os.path.join(root, fname)
                        arcname = os.path.relpath(
                            full, os.path.dirname(str(asset_path))
                        )
                        zf.write(full, arcname)
            else:
                zf.write(str(asset_path), os.path.basename(str(asset_path)))
    buf.seek(0)
    return Response(
        buf.read(),
        status_code=200,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_name}"'},
    )


def get_webdav_router() -> APIRouter:
    router = APIRouter()

    async def _resolve(
        path,
        security_scopes,
        request,
        principal,
        authn_access_tags,
        authn_scopes,
        root_tree,
        session_state,
    ):
        return await get_entry(
            path,
            security_scopes,
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            metrics=request.state.metrics,
            structure_families={
                StructureFamily.array,
                StructureFamily.sparse,
                StructureFamily.table,
                StructureFamily.container,
            },
            access_policy=getattr(request.app.state, "access_policy", None),
        )

    @router.api_route("/{path:path}", methods=["OPTIONS"], name="WebDAV OPTIONS")
    @router.api_route(
        "/", methods=["OPTIONS"], name="WebDAV OPTIONS root", include_in_schema=False
    )
    async def options(request: Request, path: str = ""):
        return Response(
            status_code=200,
            headers={"Allow": "OPTIONS, GET, HEAD, PROPFIND", "DAV": "1"},
        )

    @router.api_route("/{path:path}", methods=["PROPFIND"], name="WebDAV PROPFIND")
    @router.api_route(
        "/", methods=["PROPFIND"], name="WebDAV PROPFIND root", include_in_schema=False
    )
    async def propfind(
        request: Request,
        path: str = "",
        principal: Union[Principal, None] = Depends(get_current_principal),
        authn_access_tags: Optional[AccessTags] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["read:metadata"]),
    ):
        depth = request.headers.get("Depth", "1")
        # Build the base href for this WebDAV mount.
        base_url = str(request.base_url).rstrip("/") + "/api/webdav"
        path = path.strip("/")

        # Try the full path first; fall back to parent + asset-filename.
        try:
            entry = await _resolve(
                path,
                ["read:metadata"],
                request,
                principal,
                authn_access_tags,
                authn_scopes,
                root_tree,
                session_state,
            )
        except HTTPException as exc:
            if exc.status_code != HTTP_404_NOT_FOUND:
                raise
            entry = None

        if entry is None:
            # Path may be parent_entry/asset_filename — respond as a file leaf.
            if "/" not in path:
                raise HTTPException(
                    status_code=HTTP_404_NOT_FOUND, detail=f"Not found: {path}"
                )
            parent_path, filename = path.rsplit("/", 1)
            try:
                parent_entry = await _resolve(
                    parent_path,
                    ["read:metadata"],
                    request,
                    principal,
                    authn_access_tags,
                    authn_scopes,
                    root_tree,
                    session_state,
                )
            except HTTPException:
                raise HTTPException(
                    status_code=HTTP_404_NOT_FOUND, detail=f"Not found: {path}"
                )
            for asset in _file_assets(parent_entry):
                if _asset_filename(asset.data_uri) == filename:
                    child: dict = {
                        "href": base_url + "/" + path,
                        "displayname": filename,
                        "is_collection": asset.is_directory,
                    }
                    if not asset.is_directory:
                        try:
                            child["contentlength"] = os.path.getsize(
                                path_from_uri(asset.data_uri)
                            )
                        except OSError:
                            pass
                    return Response(
                        _build_propfind_xml([child]),
                        status_code=207,
                        media_type="application/xml; charset=utf-8",
                    )
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail=f"Asset {filename!r} not found under {parent_path!r}.",
            )

        name = path.split("/")[-1] if path else ""
        self_href = (base_url + "/" + path).rstrip("/") + "/"

        resources = []

        if entry.structure_family == StructureFamily.container:
            resources.append(
                {
                    "href": self_href,
                    "displayname": name,
                    "is_collection": True,
                    "structure_family": "container",
                }
            )
            if depth != "0":
                if hasattr(entry, "keys_range"):
                    keys = await entry.keys_range(offset=0, limit=None)
                else:
                    keys = entry.keys()
                for key in keys:
                    resources.append(
                        {
                            "href": self_href + key + "/",
                            "displayname": key,
                            "is_collection": True,
                        }
                    )

        elif entry.structure_family in {
            StructureFamily.array,
            StructureFamily.sparse,
            StructureFamily.table,
        }:
            resources.append(
                {
                    "href": self_href,
                    "displayname": name,
                    "is_collection": True,
                    "structure_family": entry.structure_family.value,
                }
            )
            if depth != "0":
                for asset in _file_assets(entry):
                    filename = _asset_filename(asset.data_uri)
                    if not filename:
                        continue
                    child = {
                        "href": self_href + filename,
                        "displayname": filename,
                        "is_collection": asset.is_directory,
                    }
                    if not asset.is_directory:
                        try:
                            child["contentlength"] = os.path.getsize(
                                path_from_uri(asset.data_uri)
                            )
                        except OSError:
                            pass
                    resources.append(child)

        return Response(
            _build_propfind_xml(resources),
            status_code=207,
            media_type="application/xml; charset=utf-8",
        )

    @router.get("/{path:path}", name="WebDAV GET")
    @router.get("/", name="WebDAV GET root", include_in_schema=False)
    async def get_resource(
        request: Request,
        path: str = "",
        principal: Union[Principal, None] = Depends(get_current_principal),
        authn_access_tags: Optional[AccessTags] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        path = path.strip("/")

        # Try resolving the full path as a Tiled entry first.
        try:
            entry = await _resolve(
                path,
                ["read:data"],
                request,
                principal,
                authn_access_tags,
                authn_scopes,
                root_tree,
                session_state,
            )
        except HTTPException as exc:
            if exc.status_code != HTTP_404_NOT_FOUND:
                raise
            entry = None

        if entry is not None:
            if entry.structure_family == StructureFamily.container:
                raise HTTPException(
                    status_code=HTTP_405_METHOD_NOT_ALLOWED,
                    detail="Cannot GET a container; use PROPFIND to list its contents.",
                )
            assets = _file_assets(entry)
            if not assets:
                raise HTTPException(
                    status_code=HTTP_404_NOT_FOUND,
                    detail="No file assets found for this entry.",
                )
            zip_name = (path.replace("/", "_") or "data") + ".zip"
            return _zip_assets(assets, zip_name)

        # Fallback: path might be parent_entry_path/asset_filename.
        if "/" not in path:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND, detail=f"Not found: {path}"
            )

        parent_path, filename = path.rsplit("/", 1)
        try:
            parent_entry = await _resolve(
                parent_path,
                ["read:data"],
                request,
                principal,
                authn_access_tags,
                authn_scopes,
                root_tree,
                session_state,
            )
        except HTTPException:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND, detail=f"Not found: {path}"
            )

        for asset in _file_assets(parent_entry):
            if _asset_filename(asset.data_uri) == filename:
                return FileResponse(str(path_from_uri(asset.data_uri)))

        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=f"Asset {filename!r} not found under {parent_path!r}.",
        )

    return router
