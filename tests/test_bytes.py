"""Tests for the `bytes` structure family: structure, adapter, and HTTP path."""

from types import SimpleNamespace

import pytest

from tiled.adapters import bytes as bytes_adapter_mod
from tiled.adapters.bytes import BytesAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.client.bytes import BytesClient
from tiled.server.app import build_app
from tiled.structures.bytes import BytesStructure
from tiled.structures.core import STRUCTURE_TYPES, StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management

PAYLOAD = b"the quick brown fox jumps over the lazy dog"

# Chunk layouts exercised by parametrized backend fixtures. Each layout is a
# tuple of bytes whose concatenation equals PAYLOAD: single, uniform multi,
# byte-at-a-time, edge-aligned (first/last byte split off), and jagged.
LAYOUTS = [
    (PAYLOAD,),
    tuple(PAYLOAD[i : i + 7] for i in range(0, len(PAYLOAD), 7)),  # noqa: E203
    tuple(PAYLOAD[i : i + 1] for i in range(len(PAYLOAD))),  # noqa: E203
    (PAYLOAD[:1], PAYLOAD[1:]),
    (PAYLOAD[:-1], PAYLOAD[-1:]),
    (PAYLOAD[:10], PAYLOAD[10:11], PAYLOAD[11:30], PAYLOAD[30:]),
]

SLICE_CASES = [
    slice(None),
    slice(0, 5),
    slice(5, 5),
    slice(5, 4),
    slice(-5, None),
    slice(None, -5),
    slice(2, 30),
    slice(0, 100),
    slice(-100, 100),
    slice(None, None, 2),
    slice(1, 20, 3),
    slice(None, None, -1),
    slice(None, None, -2),
    slice(20, 5, -1),
    slice(-1, -10, -2),
    slice(50, 60),
    slice(-100, -50),
    slice(0, 0),
    slice(100, 200),
]


@pytest.fixture(
    params=LAYOUTS, ids=lambda layout: f"chunks={tuple(len(c) for c in layout)}"
)
def chunks(request):
    return request.param


def _in_memory_adapter(chunks, tmp_path, monkeypatch):
    return BytesAdapter.from_buffers(*chunks)


def _file_adapter(chunks, tmp_path, monkeypatch):
    uris = []
    for i, chunk in enumerate(chunks):
        p = tmp_path / f"chunk_{i:03d}.bin"
        p.write_bytes(chunk)
        uris.append(p.as_uri())
    return BytesAdapter.from_uris(*uris)


def _object_store_adapter(chunks, tmp_path, monkeypatch):
    from obstore.store import MemoryStore

    store = MemoryStore()
    uris = []
    for i, chunk in enumerate(chunks):
        key = f"chunk/{i:03d}"
        store.put(key, chunk)
        uris.append(f"http://bkt/{key}")

    def fake_obstore_handle(data_uri):
        return store, data_uri.removeprefix("http://bkt/")

    def fake_size(data_uri):
        _, path = fake_obstore_handle(data_uri)
        return int(store.head(path)["size"])

    monkeypatch.setattr(bytes_adapter_mod, "_obstore_handle", fake_obstore_handle)
    monkeypatch.setattr(bytes_adapter_mod, "_size_for_uri", fake_size)
    return BytesAdapter.from_uris(*uris)


BACKENDS = {
    "in_memory": _in_memory_adapter,
    "file": _file_adapter,
    "object_store": _object_store_adapter,
}


@pytest.fixture(params=list(BACKENDS), ids=list(BACKENDS))
def adapter(request, chunks, tmp_path, monkeypatch):
    return BACKENDS[request.param](chunks, tmp_path, monkeypatch)


def _empty_node():
    return SimpleNamespace(metadata_={}, specs=[])


def _data_source(assets, size, chunks):
    return DataSource(
        structure_family=StructureFamily.bytes,
        structure=BytesStructure(size=size, chunks=chunks),
        mimetype="application/octet-stream",
        assets=assets,
        management=Management.writable,
    )


def test_structure_registered():
    assert STRUCTURE_TYPES[StructureFamily.bytes] is BytesStructure


@pytest.mark.parametrize(
    "size,chunks",
    [(11, (5, 6)), (0, ()), (3, [1, 2])],
)
def test_structure_valid(size, chunks):
    s = BytesStructure(size=size, chunks=chunks)
    assert s.size == size
    assert s.chunks == tuple(chunks)
    assert all(isinstance(c, int) for c in s.chunks)


@pytest.mark.parametrize(
    "size,chunks,match",
    [
        (10, (5, 6), "invariant violated"),
        (10, (5,), "invariant violated"),
        (10, (), "invariant violated"),
        (0, (1,), "invariant violated"),
        (-1, (), "size must be non-negative"),
        (0, (-1, 1), "chunks entries must be non-negative"),
    ],
)
def test_structure_validation_errors(size, chunks, match):
    with pytest.raises(ValueError, match=match):
        BytesStructure(size=size, chunks=chunks)


def test_init_readers_chunks_length_mismatch():
    with pytest.raises(ValueError, match="one reader per chunk"):
        BytesAdapter(
            readers=[lambda o, n: b""],
            structure=BytesStructure(size=0, chunks=()),
        )


def test_from_uris_unsupported_scheme():
    with pytest.raises(ValueError, match="unsupported scheme"):
        BytesAdapter.from_uris("ftp://example.com/foo")


def test_structure_from_json_roundtrip():
    original = BytesStructure(size=11, chunks=(5, 6))
    rebuilt = BytesStructure.from_json({"size": 11, "chunks": [5, 6]})
    assert rebuilt == original


def test_full_read(adapter, chunks):
    assert adapter.read() == PAYLOAD
    assert adapter.read(None) == PAYLOAD
    assert adapter.structure().size == len(PAYLOAD)
    assert adapter.structure().chunks == tuple(len(c) for c in chunks)


@pytest.mark.parametrize("s", SLICE_CASES, ids=repr)
def test_slice_matches_bytes_semantics(adapter, s):
    assert adapter.read(s) == PAYLOAD[s]


@pytest.mark.parametrize(
    "factory",
    [BytesAdapter.from_buffers, BytesAdapter.from_uris],
    ids=["from_buffers", "from_uris"],
)
def test_empty_adapter(factory):
    a = factory()
    assert a.read() == b""
    assert a.structure() == BytesStructure(size=0, chunks=())


def test_zero_length_chunks_preserved_and_skipped():
    a = BytesAdapter.from_buffers(b"a", b"", b"b", b"", b"c")
    assert a.structure().chunks == (1, 0, 1, 0, 1)
    assert a.read() == b"abc"
    assert a.read(slice(1, 2)) == b"b"


def test_read_only_touches_intersecting_chunks():
    layout = tuple(PAYLOAD[i : i + 7] for i in range(0, len(PAYLOAD), 7))  # noqa: E203
    calls = [0] * len(layout)

    def make_reader(idx, data):
        def read(offset, length):
            calls[idx] += 1
            return data[offset : offset + length]  # noqa: E203

        return read

    readers = [make_reader(i, c) for i, c in enumerate(layout)]
    structure = BytesStructure(size=len(PAYLOAD), chunks=tuple(len(c) for c in layout))
    a = BytesAdapter(readers, structure)

    # Bytes [14, 21) lie entirely within chunk index 2 ([14, 21)).
    assert a.read(slice(14, 21)) == PAYLOAD[14:21]
    assert calls[2] == 1
    assert sum(c for i, c in enumerate(calls) if i != 2) == 0


def test_from_uris_is_lazy(tmp_path):
    """from_uris stats files but does not read their contents eagerly."""
    p = tmp_path / "lazy.bin"
    p.write_bytes(b"abc")
    a = BytesAdapter.from_uris(p.as_uri())
    p.unlink()
    with pytest.raises(FileNotFoundError):
        a.read()


_REVERSED_LAYOUT = tuple(
    PAYLOAD[i : i + 7] for i in range(0, len(PAYLOAD), 7)  # noqa: E203
)


@pytest.mark.parametrize(
    "asset_specs, expected",
    [
        pytest.param(
            [(chunk, i) for i, chunk in reversed(list(enumerate(_REVERSED_LAYOUT)))],
            PAYLOAD,
            id="reversed_registration_explicit_num",
        ),
        pytest.param(
            [(b"AAA", None), (b"BB", 0)],
            b"BBAAA",
            id="missing_num_sorted_to_end",
        ),
        pytest.param(
            [(b"hello", 0)],
            b"hello",
            id="single_asset",
        ),
    ],
)
def test_from_catalog_ordering(tmp_path, asset_specs, expected):
    assets = []
    for i, (content, num) in enumerate(asset_specs):
        p = tmp_path / f"c{i:03d}.bin"
        p.write_bytes(content)
        assets.append(
            Asset(
                data_uri=p.as_uri(),
                is_directory=False,
                parameter="data_uris",
                num=num,
            )
        )
    # `structure.chunks` reflects the post-sort order, not registration order.
    ordered = sorted(
        asset_specs, key=lambda s: s[1] if s[1] is not None else float("inf")
    )
    chunks = tuple(len(content) for content, _ in ordered)
    ds = _data_source(assets, size=sum(chunks), chunks=chunks)
    a = BytesAdapter.from_catalog(ds, _empty_node())
    assert a.read() == expected


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
    p = tmp_path / "blob.bin"
    p.write_bytes(payload)
    data_source = DataSource(
        mimetype=mimetype,
        assets=[
            Asset(
                data_uri=p.as_uri(),
                is_directory=False,
                parameter="data_uris",
                num=0,
            )
        ],
        structure_family=StructureFamily.bytes,
        structure=BytesStructure(size=len(payload), chunks=(len(payload),)),
        management=Management.external,
    )
    return client.new(
        structure_family=StructureFamily.bytes,
        data_sources=[data_source],
        key=key,
    )


def test_register_and_read(http_client, tmp_path):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    assert isinstance(handle, BytesClient)
    assert handle.read() == PAYLOAD
    assert len(handle) == len(PAYLOAD)


def test_client_dispatch_returns_bytes_client(http_client, tmp_path):
    _register_bytes_node(http_client, tmp_path, PAYLOAD, key="b")
    assert isinstance(http_client["b"], BytesClient)


@pytest.mark.parametrize("s", SLICE_CASES, ids=repr)
def test_slice_over_http(http_client, tmp_path, s):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    assert handle.read(s) == PAYLOAD[s]
    assert handle[s] == PAYLOAD[s]


@pytest.mark.parametrize("i", [0, 1, 10, len(PAYLOAD) - 1, -1, -5])
def test_int_indexing_matches_bytes_semantics(http_client, tmp_path, i):
    """``client[i]`` returns ``int`` like ``bytes[i]``."""
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    assert handle[i] == PAYLOAD[i]


@pytest.mark.parametrize("i", [len(PAYLOAD), -len(PAYLOAD) - 1, 1000])
def test_int_index_out_of_range_raises(http_client, tmp_path, i):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    with pytest.raises(IndexError):
        handle[i]


def test_multi_chunk_assets(http_client, tmp_path):
    """Multi-asset bytes node: assets registered with `num` reassemble in order."""
    chunks = [PAYLOAD[i : i + 7] for i in range(0, len(PAYLOAD), 7)]  # noqa: E203
    assets = []
    for i, chunk in enumerate(chunks):
        p = tmp_path / f"c{i:02d}.bin"
        p.write_bytes(chunk)
        assets.append(
            Asset(
                data_uri=p.as_uri(),
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
        )
    data_source = DataSource(
        mimetype="application/octet-stream",
        # Register out of order to confirm `num` is respected.
        assets=list(reversed(assets)),
        structure_family=StructureFamily.bytes,
        structure=BytesStructure(
            size=len(PAYLOAD), chunks=tuple(len(c) for c in chunks)
        ),
        management=Management.external,
    )
    handle = http_client.new(
        structure_family=StructureFamily.bytes,
        data_sources=[data_source],
        key="multi",
    )
    assert handle.read() == PAYLOAD
    assert handle.read(slice(10, 30)) == PAYLOAD[10:30]


def test_mimetype_propagates_to_content_type(tmpdir, tmp_path):
    """A DataSource's mimetype propagates to the HTTP Content-Type."""
    catalog = in_memory(
        writable_storage=str(tmpdir),
        adapters_by_mimetype={"application/pdf": BytesAdapter},
    )
    with Context.from_app(build_app(catalog)) as ctx:
        client = from_context(ctx)
        handle = _register_bytes_node(
            client, tmp_path, b"%PDF-1.4 fake", mimetype="application/pdf"
        )
        response = client.context.http_client.get(handle.item["links"]["full"])
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/pdf")


def test_filename_sets_content_disposition(http_client, tmp_path):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    response = http_client.context.http_client.get(
        handle.item["links"]["full"], params={"filename": "payload.bin"}
    )
    cd = response.headers.get("content-disposition", "")
    assert "attachment" in cd
    assert "payload.bin" in cd
    assert "filename*=UTF-8''payload.bin" in cd  # RFC 6266 form


@pytest.mark.parametrize(
    "filename, must_not_contain",
    [
        ('evil"; attack="', '"; attack="'),  # quote injection sanitized via filename*
        ("a\r\nX-Injected: 1", "\r\n"),  # CRLF stripped
    ],
)
def test_filename_header_is_sanitized(
    http_client, tmp_path, filename, must_not_contain
):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    response = http_client.context.http_client.get(
        handle.item["links"]["full"], params={"filename": filename}
    )
    assert response.status_code == 200
    cd = response.headers.get("content-disposition", "")
    assert must_not_contain not in cd
    assert "X-Injected" not in response.headers


def test_multi_dim_slice_rejected(http_client, tmp_path):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    response = http_client.context.http_client.get(
        handle.item["links"]["full"], params={"slice": "0:10,0:5"}
    )
    assert response.status_code == 400


@pytest.mark.parametrize(
    "header, expected",
    [
        ("bytes=0-9", PAYLOAD[0:10]),
        ("bytes=5-15", PAYLOAD[5:16]),
        (f"bytes=0-{len(PAYLOAD) - 1}", PAYLOAD),
        (f"bytes=10-{len(PAYLOAD) + 100}", PAYLOAD[10:]),  # hi clamped to size
        ("bytes=20-", PAYLOAD[20:]),
        ("bytes=-5", PAYLOAD[-5:]),
        (f"bytes=-{len(PAYLOAD) * 2}", PAYLOAD),  # suffix > size clamps to start
    ],
)
def test_range_returns_206(http_client, tmp_path, header, expected):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    response = http_client.context.http_client.get(
        handle.item["links"]["full"], headers={"Range": header}
    )
    assert response.status_code == 206
    assert response.content == expected
    assert response.headers["accept-ranges"] == "bytes"
    content_range = response.headers["content-range"]
    assert content_range.startswith("bytes ")
    assert content_range.endswith(f"/{len(PAYLOAD)}")


@pytest.mark.parametrize(
    "header",
    [
        f"bytes={len(PAYLOAD)}-{len(PAYLOAD) + 10}",  # lo == size
        f"bytes={len(PAYLOAD) + 1}-",  # lo > size
        "bytes=10-5",  # inverted range
    ],
)
def test_range_unsatisfiable_returns_416(http_client, tmp_path, header):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    response = http_client.context.http_client.get(
        handle.item["links"]["full"], headers={"Range": header}
    )
    assert response.status_code == 416
    assert response.headers["content-range"] == f"bytes */{len(PAYLOAD)}"


@pytest.mark.parametrize(
    "header",
    [
        "items=0-9",  # wrong unit
        "bytes=abc-def",  # not integers
        "bytes=",  # empty spec
        "bytes=0-9,20-29",  # multi-range (not supported)
        "bytes=-0",  # zero-length suffix
    ],
)
def test_range_malformed_serves_full_payload(http_client, tmp_path, header):
    """Malformed Range headers fall through to a 200 full response (per RFC 9110)."""
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    response = http_client.context.http_client.get(
        handle.item["links"]["full"], headers={"Range": header}
    )
    assert response.status_code == 200
    assert response.content == PAYLOAD


def test_accept_ranges_advertised_on_full_response(http_client, tmp_path):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    response = http_client.context.http_client.get(handle.item["links"]["full"])
    assert response.status_code == 200
    assert response.headers["accept-ranges"] == "bytes"


def test_range_wins_over_slice_param(http_client, tmp_path):
    """If both Range and ?slice= are present, Range takes precedence."""
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    response = http_client.context.http_client.get(
        handle.item["links"]["full"],
        headers={"Range": "bytes=0-4"},
        params={"slice": "20:30"},
    )
    assert response.status_code == 206
    assert response.content == PAYLOAD[0:5]


def test_links_contain_full(http_client, tmp_path):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    assert "full" in handle.item["links"]
    assert "/bytes/full/" in handle.item["links"]["full"]
    assert handle.item["links"]["full"].endswith("blob")


def test_unsupported_route_path_404(http_client):
    response = http_client.context.http_client.get("/api/v1/bytes/full/does-not-exist")
    assert response.status_code == 404


def test_repr(http_client, tmp_path):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    assert "BytesClient" in repr(handle)
    assert str(len(PAYLOAD)) in repr(handle)


# --- Parallel export -------------------------------------------------------


@pytest.mark.parametrize("workers", [1, 2, 4, 16])
def test_export_single_asset(http_client, tmp_path, workers):
    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    out = tmp_path / f"out_w{workers}.bin"
    handle.export(out, workers=workers)
    assert out.read_bytes() == PAYLOAD


@pytest.mark.parametrize("workers", [1, 4])
def test_export_multi_chunk(http_client, tmp_path, workers):
    """Multi-chunk payload: parallel ranges align to chunk boundaries."""
    chunks = [PAYLOAD[i : i + 7] for i in range(0, len(PAYLOAD), 7)]  # noqa: E203
    assets = []
    for i, chunk in enumerate(chunks):
        p = tmp_path / f"c{i:02d}.bin"
        p.write_bytes(chunk)
        assets.append(
            Asset(
                data_uri=p.as_uri(),
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
        )
    data_source = DataSource(
        mimetype="application/octet-stream",
        assets=assets,
        structure_family=StructureFamily.bytes,
        structure=BytesStructure(
            size=len(PAYLOAD), chunks=tuple(len(c) for c in chunks)
        ),
        management=Management.external,
    )
    handle = http_client.new(
        structure_family=StructureFamily.bytes,
        data_sources=[data_source],
        key=f"multi_w{workers}",
    )
    out = tmp_path / f"multi_w{workers}.bin"
    handle.export(out, workers=workers)
    assert out.read_bytes() == PAYLOAD


def test_export_empty_payload(http_client, tmp_path):
    handle = _register_bytes_node(http_client, tmp_path, b"", key="empty")
    out = tmp_path / "empty.bin"
    handle.export(out, workers=4)
    assert out.read_bytes() == b""


def test_export_to_buffer_ignores_workers(http_client, tmp_path):
    """A non-path destination forces the single-shot path."""
    import io

    handle = _register_bytes_node(http_client, tmp_path, PAYLOAD)
    buf = io.BytesIO()
    handle.export(buf, format="application/octet-stream", workers=8)
    assert buf.getvalue() == PAYLOAD


def test_partition_aligns_to_chunks():
    """Multi-chunk payloads partition along chunk boundaries regardless of workers."""
    from tiled.client.bytes import _partition

    assert _partition(20, (5, 7, 8), workers=16) == [(0, 5), (5, 12), (12, 20)]


@pytest.mark.parametrize(
    "size, workers, expected",
    [
        (10, 1, [(0, 10)]),
        (10, 4, [(0, 3), (3, 6), (6, 9), (9, 10)]),
        (10, 3, [(0, 4), (4, 8), (8, 10)]),
        (0, 4, [(0, 0)]),
    ],
)
def test_partition_single_chunk_even_split(size, workers, expected):
    from tiled.client.bytes import _partition

    assert _partition(size, (size,), workers) == expected
