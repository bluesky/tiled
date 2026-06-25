"""Tests for the `bytes` structure family: structure, adapter, registration.

The `bytes` family has no dedicated content endpoint. Clients download the
underlying assets via `/asset/bytes/{path}?id=N`, gated by
`settings.expose_raw_assets`. These tests cover structure registration,
the minimal adapter, catalog wiring, and the `/asset/bytes` path.
"""

from types import SimpleNamespace

import pytest

from tiled.adapters.bytes import BytesAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.structures.bytes import BytesStructure
from tiled.structures.core import STRUCTURE_TYPES, StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management

PAYLOAD = b"the quick brown fox jumps over the lazy dog"


# --- structure & adapter unit tests ---------------------------------------


def test_structure_registered():
    assert STRUCTURE_TYPES[StructureFamily.bytes] is BytesStructure


def test_structure_is_empty_dataclass():
    """BytesStructure carries no fields; size lives on each Asset."""
    s = BytesStructure()
    assert s == BytesStructure()
    # No size/chunks attributes
    assert not hasattr(s, "size")
    assert not hasattr(s, "chunks")


def test_structure_from_json_ignores_extra_keys():
    """from_json round-trips an empty structure regardless of payload."""
    assert BytesStructure.from_json({}) == BytesStructure()
    # Tolerate stray keys from older catalogs.
    assert BytesStructure.from_json({"size": 10, "chunks": [10]}) == BytesStructure()


def test_adapter_structure_family():
    assert BytesAdapter.structure_family == StructureFamily.bytes


def test_adapter_holds_structure():
    s = BytesStructure()
    a = BytesAdapter(s)
    assert a.structure() is s
    assert a.structure_family == StructureFamily.bytes


def test_adapter_metadata_and_specs_default_empty():
    a = BytesAdapter(BytesStructure())
    assert a.metadata() == {}
    assert a.specs == []


def test_adapter_supported_storage_includes_file_and_object():
    from tiled.storage import FileStorage, ObjectStorage

    assert BytesAdapter.supported_storage() == {FileStorage, ObjectStorage}


def test_from_catalog_builds_from_data_source():
    structure = BytesStructure()
    ds = SimpleNamespace(structure=structure, assets=[])
    node = SimpleNamespace(metadata_={"foo": "bar"}, specs=[])
    a = BytesAdapter.from_catalog(ds, node)
    assert a.structure() is structure
    assert a.metadata() == {"foo": "bar"}


def test_adapter_has_no_read_methods():
    """The bytes adapter is metadata-only; content downloads go through /asset/bytes."""
    a = BytesAdapter(BytesStructure())
    assert not hasattr(a, "read")
    assert not hasattr(a, "read_stream")


# --- HTTP end-to-end -------------------------------------------------------


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


def test_register_node_creates_bytes_family(http_client, tmp_path):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    assert handle.item["attributes"]["structure_family"] == "bytes"
    # Size now lives on the asset, not the structure.
    response = http_client.context.http_client.get(
        "/api/v1/metadata/blob", params={"include_data_sources": True}
    )
    ds = response.json()["data"]["attributes"]["data_sources"][0]
    assert ds["assets"][0]["size"] == len(PAYLOAD)


def test_node_links_omit_full(http_client, tmp_path):
    """Bytes nodes expose only `self`; no /bytes/full route exists."""
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    links = handle.item["links"]
    assert "self" in links
    assert "full" not in links


def test_node_returns_base_client(http_client, tmp_path):
    """No dedicated BytesClient; bytes nodes use the generic BaseClient."""
    from tiled.client.base import BaseClient

    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    assert type(handle) is BaseClient


def test_bytes_full_route_is_gone(http_client, tmp_path):
    """The old /bytes/full/{path} endpoint no longer exists."""
    _register_bytes_node(http_client, tmp_path, PAYLOAD)
    response = http_client.context.http_client.get("/api/v1/bytes/full/blob")
    assert response.status_code == 404


def test_download_via_asset_bytes(http_client, tmp_path):
    """Bytes content is downloaded one asset at a time via /asset/bytes."""
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD, key="blob")
    # Discover the asset id from the node metadata.
    response = http_client.context.http_client.get(
        "/api/v1/metadata/blob", params={"include_data_sources": True}
    )
    ds = response.json()["data"]["attributes"]["data_sources"][0]
    asset_id = ds["assets"][0]["id"]
    # Download the asset.
    response = http_client.context.http_client.get(
        "/api/v1/asset/bytes/blob", params={"id": asset_id}
    )
    assert response.status_code == 200
    assert response.content == PAYLOAD
    # Sanity: the handle's path is what we expect.
    assert handle.item["id"] == "blob"


def test_asset_bytes_gated_by_expose_raw_assets(tmpdir, tmp_path):
    """With `expose_raw_assets=False`, /asset/bytes returns 403."""
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog, server_settings={"expose_raw_assets": False})
    with Context.from_app(app) as ctx:
        client = from_context(ctx)
        _register_bytes_node(client, tmp_path, PAYLOAD, key="blob")
        # Find the asset id first (metadata access is unrestricted).
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


def test_multi_asset_node_records_per_asset_size(http_client, tmp_path):
    """A multi-asset bytes node records size on each asset; each is downloadable."""
    chunks = [PAYLOAD[i : i + 7] for i in range(0, len(PAYLOAD), 7)]  # noqa: E203
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
    data_source = DataSource(
        mimetype="application/octet-stream",
        assets=list(reversed(assets)),  # registered out of order; `num` defines order
        structure_family=StructureFamily.bytes,
        structure=BytesStructure(),
        management=Management.external,
    )
    http_client.new(
        structure_family=StructureFamily.bytes,
        data_sources=[data_source],
        key="multi",
    )

    response = http_client.context.http_client.get(
        "/api/v1/metadata/multi", params={"include_data_sources": True}
    )
    ds = response.json()["data"]["attributes"]["data_sources"][0]
    sorted_assets = sorted(ds["assets"], key=lambda a: a["num"])
    # Per-asset size is reported.
    assert [a["size"] for a in sorted_assets] == [len(c) for c in chunks]
    # Fetch each asset by id and concatenate.
    parts = []
    for asset in sorted_assets:
        r = http_client.context.http_client.get(
            "/api/v1/asset/bytes/multi", params={"id": asset["id"]}
        )
        assert r.status_code == 200
        parts.append(r.content)
    assert b"".join(parts) == PAYLOAD


def test_default_mimetype_is_octet_stream(http_client, tmp_path):
    """Default mimetype for unstructured payloads."""
    _register_bytes_node(http_client, tmp_path, PAYLOAD, key="blob")
    response = http_client.context.http_client.get(
        "/api/v1/metadata/blob", params={"include_data_sources": True}
    )
    ds = response.json()["data"]["attributes"]["data_sources"][0]
    assert ds["mimetype"] == "application/octet-stream"


def test_registers_with_pdf_mimetype(tmpdir, tmp_path):
    """Bytes is the fallback for any unknown-structure mimetype."""
    catalog = in_memory(
        writable_storage=str(tmpdir),
        adapters_by_mimetype={"application/pdf": BytesAdapter},
    )
    with Context.from_app(build_app(catalog)) as ctx:
        client = from_context(ctx)
        _register_bytes_node(
            client, tmp_path, b"%PDF-1.4 fake", mimetype="application/pdf", key="doc"
        )
        response = client.context.http_client.get(
            "/api/v1/metadata/doc", params={"include_data_sources": True}
        )
        ds = response.json()["data"]["attributes"]["data_sources"][0]
        assert ds["mimetype"] == "application/pdf"
