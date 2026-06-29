"""Integration tests for the `bytes` structure family.

The `bytes` family has no dedicated content endpoint and no family-specific
client. Clients register an opaque payload, then download each underlying
asset through `/asset/bytes/{path}?id=N`, gated by `settings.expose_raw_assets`.
These tests exercise that full round-trip end-to-end.
"""

import warnings
from pathlib import Path

import pytest

from tiled.adapters.bytes import BytesAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.structures.bytes import BytesStructure
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management

PAYLOAD = b"the quick brown fox jumps over the lazy dog"


@pytest.fixture
def http_client(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(catalog)) as ctx:
        yield from_context(ctx)


def _register_bytes_node(
    client, tmp_path, payload, key="blob", mimetype="application/octet-stream"
):
    """Write `payload` to a file and register it as an external bytes node."""
    p = tmp_path / f"{key}.bin"
    p.write_bytes(payload)
    data_source = DataSource(
        mimetype=mimetype,
        assets=[
            Asset(
                data_uri=p.as_uri(),
                is_directory=False,
                size=len(payload),
                parameter="data_uris",
                num=0,
            )
        ],
        structure_family=StructureFamily.bytes,
        structure=BytesStructure(),
        management=Management.external,
    )
    return client.new(
        structure_family=StructureFamily.bytes,
        data_sources=[data_source],
        key=key,
    )


def _register_multi_asset_node(client, tmp_path, chunks, key="multi", reverse=False):
    """Write each chunk to its own file and register them as one bytes node.

    `num` is set to the chunk's index, so the original payload can be recovered
    by ordering downloaded parts by `num`. When `reverse=True`, the assets are
    submitted in the *opposite* order to verify the server canonicalises by `num`.
    """
    assets = []
    for i, chunk in enumerate(chunks):
        p = tmp_path / f"c{i:02d}.bin"
        p.write_bytes(chunk)
        assets.append(
            Asset(
                data_uri=p.as_uri(),
                is_directory=False,
                size=len(chunk),
                parameter="data_uris",
                num=i,
            )
        )
    client.new(
        structure_family=StructureFamily.bytes,
        data_sources=[
            DataSource(
                mimetype="application/octet-stream",
                assets=list(reversed(assets)) if reverse else assets,
                structure_family=StructureFamily.bytes,
                structure=BytesStructure(),
                management=Management.external,
            )
        ],
        key=key,
    )


class _DiskDestination:
    """`raw_export` target backed by a directory on disk."""

    def __init__(self, tmp_path):
        self.dest = tmp_path / "out"
        self.dest.mkdir()

    @property
    def arg(self):
        return self.dest

    def read(self, handle):
        return Path(handle).read_bytes()

    def asset_id(self, handle):
        # Multi-asset disk layout: `<dest>/<asset_id>/<filename>`.
        return int(Path(handle).parent.name)


class _MappingDestination:
    """`raw_export` target backed by an in-memory `dict` of `BytesIO` buffers."""

    def __init__(self, tmp_path):
        self.buffers = {}

    @property
    def arg(self):
        return self.buffers

    def read(self, handle):
        return self.buffers[handle].read()

    def asset_id(self, handle):
        # Multi-asset mapping layout: `<asset_id>/<filename>`.
        return int(handle.split("/")[0])


@pytest.fixture(params=["disk", "mapping"])
def destination(request, tmp_path):
    return {"disk": _DiskDestination, "mapping": _MappingDestination}[request.param](
        tmp_path
    )


def test_roundtrip_register_and_download(http_client, tmp_path):
    """Register a bytes node, inspect its metadata, and download the asset.

    Covers the full path: structure_family wiring, default mimetype,
    Asset.size population, asset-id discovery, and content round-trip via /asset/bytes.
    """
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD, key="blob")
    assert handle.item["attributes"]["structure_family"] == "bytes"

    meta = http_client.context.http_client.get(
        "/api/v1/metadata/blob", params={"include_data_sources": True}
    )
    ds = meta.json()["data"]["attributes"]["data_sources"][0]
    assert ds["mimetype"] == "application/octet-stream"
    asset = ds["assets"][0]
    assert asset["size"] == len(PAYLOAD)

    response = http_client.context.http_client.get(
        "/api/v1/asset/bytes/blob", params={"id": asset["id"]}
    )
    assert response.status_code == 200
    assert response.content == PAYLOAD


def test_multi_asset_roundtrip(http_client, tmp_path):
    """A bytes node backed by multiple assets reports each size and is downloadable
    in `num` order; concatenating the parts reconstructs the original payload."""
    chunks = [PAYLOAD[i : i + 7] for i in range(0, len(PAYLOAD), 7)]  # noqa: E203
    # Registered out of order; `num` defines the canonical order.
    _register_multi_asset_node(http_client, tmp_path, chunks, key="multi", reverse=True)

    meta = http_client.context.http_client.get(
        "/api/v1/metadata/multi", params={"include_data_sources": True}
    )
    ds = meta.json()["data"]["attributes"]["data_sources"][0]
    sorted_assets = sorted(ds["assets"], key=lambda a: a["num"])
    assert [a["size"] for a in sorted_assets] == [len(c) for c in chunks]

    parts = []
    for asset in sorted_assets:
        r = http_client.context.http_client.get(
            "/api/v1/asset/bytes/multi", params={"id": asset["id"]}
        )
        assert r.status_code == 200
        parts.append(r.content)
    assert b"".join(parts) == PAYLOAD


def test_asset_bytes_gated_by_expose_raw_assets(tmpdir, tmp_path):
    """With `expose_raw_assets=False`, /asset/bytes returns 403 even though
    metadata access (and therefore asset-id discovery) remains unrestricted."""
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog, server_settings={"expose_raw_assets": False})
    with Context.from_app(app) as ctx:
        client = from_context(ctx)
        _register_bytes_node(client, tmp_path, PAYLOAD, key="blob")
        meta = client.context.http_client.get(
            "/api/v1/metadata/blob", params={"include_data_sources": True}
        )
        asset_id = meta.json()["data"]["attributes"]["data_sources"][0]["assets"][0][
            "id"
        ]
        response = client.context.http_client.get(
            "/api/v1/asset/bytes/blob", params={"id": asset_id}
        )
        assert response.status_code == 403


def test_registers_with_arbitrary_mimetype(tmpdir, tmp_path):
    """Bytes is the fallback adapter for any unknown-structure mimetype; mapping
    `application/pdf` to BytesAdapter is enough to register PDFs as bytes nodes."""
    catalog = in_memory(
        writable_storage=str(tmpdir),
        adapters_by_mimetype={"application/pdf": BytesAdapter},
    )
    with Context.from_app(build_app(catalog)) as ctx:
        client = from_context(ctx)
        _register_bytes_node(
            client, tmp_path, b"%PDF-1.4 fake", mimetype="application/pdf", key="doc"
        )
        meta = client.context.http_client.get(
            "/api/v1/metadata/doc", params={"include_data_sources": True}
        )
        ds = meta.json()["data"]["attributes"]["data_sources"][0]
        assert ds["mimetype"] == "application/pdf"
        assert ds["structure_family"] == "bytes"


def test_raw_export_single_asset(http_client, tmp_path, destination):
    """`BaseClient.raw_export()` writes a single-asset bytes payload to one handle.

    Bytes nodes return the generic `BaseClient` (no family-specific client), so
    `raw_export` is the high-level user-facing download API. The destination may
    be either a directory on disk or an in-memory `MutableMapping`; in both
    cases a single-asset node produces exactly one handle keyed by the
    server-supplied filename.
    """
    _register_bytes_node(http_client, tmp_path, PAYLOAD, key="blob")
    handles = http_client["blob"].raw_export(destination.arg)
    assert len(handles) == 1
    assert Path(handles[0]).name == "blob.bin"
    assert destination.read(handles[0]) == PAYLOAD


def test_raw_export_multi_asset(http_client, tmp_path, destination):
    """`raw_export()` on a multi-asset bytes node namespaces handles by asset id.

    On disk each asset lands in `<dest>/<asset_id>/<filename>`; in a mapping the
    keys take the form `<asset_id>/<filename>`. In either layout, ordering the
    handles by the asset's server-side `num` reproduces the original payload.
    """
    chunks = [PAYLOAD[i : i + 7] for i in range(0, len(PAYLOAD), 7)]  # noqa: E203
    _register_multi_asset_node(http_client, tmp_path, chunks, key="multi")
    handles = http_client["multi"].raw_export(destination.arg)
    assert len(handles) == len(chunks)
    ds = http_client.context.http_client.get(
        "/api/v1/metadata/multi", params={"include_data_sources": True}
    ).json()["data"]["attributes"]["data_sources"][0]
    num_by_id = {a["id"]: a["num"] for a in ds["assets"]}
    ordered = sorted(handles, key=lambda h: num_by_id[destination.asset_id(h)])
    assert b"".join(destination.read(h) for h in ordered) == PAYLOAD


def test_put_asset_backfills_size_on_reregistration(http_client, tmp_path):
    """Re-registering an asset whose stored `size` is stale (or NULL) updates
    it. `_put_asset` matches existing assets by `data_uri`; when the new
    record carries a non-null `size` that differs from the stored value,
    the row is updated. This lets a re-`register` repair pre-existing rows
    that predate `Asset.size` being populated by the client."""
    # First registration: register with size=None to simulate a legacy row.
    p = tmp_path / "blob.bin"
    p.write_bytes(PAYLOAD)
    http_client.new(
        structure_family=StructureFamily.bytes,
        data_sources=[
            DataSource(
                mimetype="application/octet-stream",
                assets=[
                    Asset(
                        data_uri=p.as_uri(),
                        is_directory=False,
                        size=None,
                        parameter="data_uris",
                        num=0,
                    )
                ],
                structure_family=StructureFamily.bytes,
                structure=BytesStructure(),
                management=Management.external,
            )
        ],
        key="legacy",
    )
    meta = http_client.context.http_client.get(
        "/api/v1/metadata/legacy", params={"include_data_sources": True}
    ).json()
    assert meta["data"]["attributes"]["data_sources"][0]["assets"][0]["size"] is None

    # Re-register a new node referencing the same data_uri, this time with
    # size populated. The shared underlying asset row should be updated.
    http_client.new(
        structure_family=StructureFamily.bytes,
        data_sources=[
            DataSource(
                mimetype="application/octet-stream",
                assets=[
                    Asset(
                        data_uri=p.as_uri(),
                        is_directory=False,
                        size=len(PAYLOAD),
                        parameter="data_uris",
                        num=0,
                    )
                ],
                structure_family=StructureFamily.bytes,
                structure=BytesStructure(),
                management=Management.external,
            )
        ],
        key="refreshed",
    )
    for key in ("legacy", "refreshed"):
        meta = http_client.context.http_client.get(
            f"/api/v1/metadata/{key}", params={"include_data_sources": True}
        ).json()
        asset = meta["data"]["attributes"]["data_sources"][0]["assets"][0]
        assert asset["size"] == len(PAYLOAD)


def test_raw_export_destination_directory_kwarg_is_deprecated(http_client, tmp_path):
    """The old `destination_directory` keyword still works but emits
    `DeprecationWarning`. Passing both the new and old names raises."""
    _register_bytes_node(http_client, tmp_path, PAYLOAD, key="blob")
    dest = tmp_path / "out"
    dest.mkdir()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        paths = http_client["blob"].raw_export(destination_directory=dest)
    assert len(paths) == 1
    assert Path(paths[0]).read_bytes() == PAYLOAD
    assert any(
        issubclass(w.category, DeprecationWarning)
        and "destination_directory" in str(w.message)
        for w in caught
    )

    with pytest.raises(TypeError, match="both 'destination'"):
        http_client["blob"].raw_export(dest, destination_directory=dest)


def test_raw_export_handles_missing_content_length(http_client, tmp_path):
    """Regression: large compressed responses have no `Content-Length` header
    (the compression middleware strips it when streaming a chunked body).
    `raw_export` must not crash with `KeyError('Content-Length')` and should
    show an indeterminate progress bar instead."""
    # A payload comfortably larger than the compression middleware's
    # minimum_size (500 bytes) so the server will actually compress and stream.
    large_payload = (b"the quick brown fox " * 1024) * 64  # ~1.3 MiB
    p = tmp_path / "big.bin"
    p.write_bytes(large_payload)
    http_client.new(
        structure_family=StructureFamily.bytes,
        data_sources=[
            DataSource(
                mimetype="application/octet-stream",
                assets=[
                    Asset(
                        data_uri=p.as_uri(),
                        is_directory=False,
                        size=len(large_payload),
                        parameter="data_uris",
                        num=0,
                    )
                ],
                structure_family=StructureFamily.bytes,
                structure=BytesStructure(),
                management=Management.external,
            )
        ],
        key="big",
    )
    # Force the server's compression middleware to engage and strip
    # Content-Length on the streaming branch.
    http_client.context.http_client.headers["Accept-Encoding"] = "zstd"
    dest = tmp_path / "out"
    dest.mkdir()
    paths = http_client["big"].raw_export(dest)
    assert len(paths) == 1
    assert Path(paths[0]).read_bytes() == large_payload
