"""Tests for the `bytes` structure family: `BytesStructure` and `BytesAdapter`."""

from types import SimpleNamespace

import pytest

from tiled.adapters import bytes as bytes_adapter_mod
from tiled.adapters.bytes import BytesAdapter
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
