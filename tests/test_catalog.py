import asyncio
import random
import string
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
from typing import cast

import dask.array
import numpy
import pandas
import pandas.testing
import pyarrow
import pytest
import pytest_asyncio
import sqlalchemy.exc
import tifffile
import xarray
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool, QueuePool, StaticPool

from tiled.adapters.csv import CSVAdapter
from tiled.adapters.dataframe import ArrayAdapter
from tiled.adapters.tiff import TiffAdapter
from tiled.catalog import in_memory
from tiled.catalog.adapter import WouldDeleteData
from tiled.catalog.explain import record_explanations
from tiled.client import Context, from_context
from tiled.client.register import register
from tiled.client.utils import ClientError
from tiled.client.xarray import write_xarray_dataset
from tiled.queries import Eq, Key
from tiled.server.app import build_app, build_app_from_config
from tiled.server.schemas import Asset, DataSource, Management
from tiled.storage import SQLStorage, get_storage, parse_storage, sanitize_uri
from tiled.structures.array import ArrayStructure, BuiltinDtype
from tiled.structures.core import StructureFamily
from tiled.utils import Conflicts, ensure_specified_sql_driver, ensure_uri

from .utils import sql_table_exists


@pytest_asyncio.fixture
async def a(catalog_adapter):
    "Raw adapter, not to be used within an app because it is manually started and stopped."
    await catalog_adapter.startup()
    yield catalog_adapter
    await catalog_adapter.shutdown()


@pytest_asyncio.fixture
async def client(catalog_adapter):
    app = build_app(catalog_adapter)
    with Context.from_app(app) as context:
        yield from_context(context)


@pytest.mark.asyncio
async def test_nested_node_creation(a):
    await a.create_node(
        key="b",
        metadata={},
        structure_family=StructureFamily.container,
        specs=[],
    )
    b = await a.lookup_adapter(["b"])
    await b.create_node(
        key="c",
        metadata={},
        structure_family=StructureFamily.container,
        specs=[],
    )
    c = await b.lookup_adapter(["c"])
    assert await b.path_segments() == ["b"]
    assert await c.path_segments() == ["b", "c"]
    assert (await a.keys_page(limit=1))[0] == ["b"]
    assert (await b.keys_page(limit=1))[0] == ["c"]
    # smoke test
    await a.items_page(limit=1)
    await b.items_page(limit=1)
    await a.shutdown()


@pytest.mark.asyncio
async def test_sorting(a):
    # Generate lists of letters and numbers, randomly shuffled.
    random_state = random.Random(0)
    ordered_letters = list(string.ascii_lowercase[:10])
    shuffled_letters = list(ordered_letters)
    random_state.shuffle(shuffled_letters)
    shuffled_numbers = [0] * 5 + [1] * 5
    random_state.shuffle(shuffled_numbers)
    assert ordered_letters != shuffled_letters
    assert sorted(shuffled_letters) != shuffled_letters

    for letter, number in zip(shuffled_letters, shuffled_numbers):
        await a.create_node(
            key=letter,
            metadata={"letter": letter, "number": number},
            structure_family=StructureFamily.container,
            specs=[],
        )

    # Default sorting is _not_ ordered.
    default_key_order = (await a.keys_page(limit=10))[0]
    assert default_key_order != ordered_letters
    assert set(default_key_order) == set(ordered_letters)
    # Sorting by ("", -1) gives reversed default order.
    reversed_default_key_order = (await a.sort([("", -1)]).keys_page(limit=10))[0]
    assert reversed_default_key_order == list(reversed(default_key_order))

    # Sort by key.
    assert (await a.sort([("id", 1)]).keys_page(limit=10))[0] == ordered_letters
    # Test again, with items_page.
    assert [
        k for k, v in (await a.sort([("id", 1)]).items_page(limit=10))[0]
    ] == ordered_letters

    # Sort by letter metadata.
    # Use explicit 'metadata.{key}' namespace.
    assert (await a.sort([("metadata.letter", 1)]).keys_page(limit=10))[
        0
    ] == ordered_letters

    # Sort by letter metadata.
    # Use implicit '{key}' (more convenient, and necessary for back-compat).
    assert (await a.sort([("letter", 1)]).keys_page(limit=10))[0] == ordered_letters

    # Sort by number and then by letter.
    # Use explicit 'metadata.{key}' namespace.
    items = await a.sort([("metadata.number", 1), ("metadata.letter", 1)]).items_page(
        limit=10
    )
    items = items[0]
    numbers = [v.metadata()["number"] for k, v in items]
    letters = [v.metadata()["letter"] for k, v in items]
    keys = [k for k, v in items]
    # Numbers are sorted.
    numbers = sorted(numbers)
    # Within each block of numbers, keys and letters are sorted.
    assert sorted(keys[:5]) == keys[:5] == letters[:5]
    assert sorted(keys[5:]) == keys[5:] == letters[5:]


@pytest.mark.asyncio
async def test_search(a):
    for letter, number in zip(string.ascii_lowercase[:5], range(5)):
        await a.create_node(
            key=letter,
            metadata={"letter": letter, "number": number, "x": {"y": {"z": letter}}},
            structure_family=StructureFamily.container,
            specs=[],
        )
    assert "c" in (await a.keys_page(limit=5))[0]
    assert (await a.search(Eq("letter", "c")).keys_page(limit=5))[0] == ["c"]
    assert (await a.search(Eq("number", 2)).keys_page(limit=5))[0] == ["c"]

    # Looking up "d" inside search results should find nothing when
    # "d" is filtered out by a search query first.
    assert await a.lookup_adapter(["d"]) is not None
    with pytest.raises(KeyError):
        await a.search(Eq("letter", "c")).lookup_adapter(["d"])

    # Search on nested key.
    assert (await a.search(Eq("x.y.z", "c")).keys_page(limit=5))[0] == ["c"]
    # Created nested nodes and search on them.
    d = await a.lookup_adapter(["d"])
    for letter, number in zip(string.ascii_lowercase[:5], range(10, 15)):
        await d.create_node(
            key=letter,
            metadata={"letter": letter, "number": number},
            structure_family=StructureFamily.container,
            specs=[],
        )
    assert (await d.search(Eq("letter", "c")).keys_page(limit=5))[0] == ["c"]
    assert (await d.search(Eq("number", 12)).keys_page(limit=5))[0] == ["c"]


@pytest.mark.asyncio
async def test_metadata_index_is_used(example_data_adapter):
    a = example_data_adapter  # for succinctness below
    # Check that an index is used by inspecting the content of an 'EXPLAIN ...'
    # query. The exact content is intended for humans and is not an API, but we
    # can coarsely check that the index of interest is mentioned.
    # The 'top_level_metadata' GIN index is PostgreSQL-only. SQLite uses a
    # B-tree covering index instead; we just verify any index is used.
    dialect = a.context.engine.url.get_dialect().name
    if dialect == "postgresql":
        expected_index = "top_level_metadata"
    else:
        expected_index = "nodes_parent"  # B-tree index on parent (name varies by Alembic/SQLite version)
    await a.startup()
    with record_explanations() as e:
        results = await a.search(Key("number_as_string") == "3").keys_page(limit=5)
        assert len(results[0]) == 1
        assert expected_index in str(e)
    with record_explanations() as e:
        results = await a.search(Key("number") == 3).keys_page(limit=5)
        assert len(results[0]) == 1
        assert expected_index in str(e)
    with record_explanations() as e:
        results = await a.search(Key("bool") == False).keys_page(limit=5)  # noqa: #712
        assert len(results[0]) == 1
        assert expected_index in str(e)
    with record_explanations() as e:
        results = await a.search(Key("nested.number_as_string") == "3").keys_page(
            limit=5
        )
        assert len(results[0]) == 1
        assert expected_index in str(e)
    with record_explanations() as e:
        results = await a.search(Key("nested.number") == 3).keys_page(limit=5)
        assert len(results[0]) == 1
        assert expected_index in str(e)
    with record_explanations() as e:
        results = await a.search(Key("nested.bool") == False).keys_page(  # noqa: #712
            limit=5
        )
        assert len(results[0]) == 1
        assert expected_index in str(e)
    await a.shutdown()


@pytest.mark.asyncio
async def test_write_array_external(a, tmpdir):
    arr = numpy.ones((5, 3))
    filepath = str(tmpdir / "file.tiff")
    data_uri = ensure_uri(filepath)
    tifffile.imwrite(filepath, arr)
    ad = TiffAdapter(data_uri)
    structure = asdict(ad.structure())
    await a.create_node(
        key="x",
        structure_family="array",
        metadata={},
        data_sources=[
            DataSource(
                structure_family="array",
                mimetype="image/tiff",
                structure=structure,
                parameters={},
                properties={"chunks": structure["chunks"]},  # Optional property
                management="external",
                assets=[
                    Asset(
                        parameter="data_uri",
                        num=None,
                        data_uri=str(data_uri),
                        is_directory=False,
                    )
                ],
            )
        ],
    )
    x = await a.lookup_adapter(["x"])
    assert numpy.array_equal(await x.read(), arr)
    assert (await x.data_sources())[0].properties == {"chunks": [[5], [3]]}


# A reshaped file sequence: K = P*Q*R files, each an M x N image, whose data
# source structure declares the logical shape (P, Q, R, M, N). The catalog's
# lazy per-frame path must map an arbitrary (possibly strided, ellipsis-bearing)
# slice on that 5-D shape back to exactly the file indices it touches.
_RESHAPE_P, _RESHAPE_Q, _RESHAPE_R, _RESHAPE_M, _RESHAPE_N = 2, 3, 4, 5, 7


@pytest.mark.parametrize(
    "slice_input",
    [
        Ellipsis,  # full read -> every file
        1,  # single leftmost index
        (slice(0, 2), 1, slice(0, 4, 2)),  # strided along the R (stacking) axis
        (slice(None), slice(1, 3), slice(0, 4, 2), Ellipsis, slice(0, 4)),
        (Ellipsis, 0, 0, 0),
        (0, Ellipsis, slice(0, 3)),
        (slice(0, 2), slice(0, 3), slice(1, 4, 2)),
        (slice(0, 1), slice(0, 2), slice(0, 2), slice(0, 2), slice(0, 3)),
    ],
)
@pytest.mark.asyncio
async def test_lazy_reshaped_sequence_selects_correct_assets(tmpdir, slice_input):
    """The lazy per-frame path resolves exactly the assets a reshaped-sequence
    slice needs, and reads back data identical to the eager reference.

    Files are stored MxN but the structure declares (P, Q, R, M, N); this
    exercises the reshape branch of `file_indices_for_slice`/`read` for
    complex slices (strides, ellipsis, integer index reduction).
    """
    P, Q, R, M, N = _RESHAPE_P, _RESHAPE_Q, _RESHAPE_R, _RESHAPE_M, _RESHAPE_N
    K = P * Q * R

    # Distinct data per file so any mis-selection surfaces as a value mismatch.
    rng = numpy.random.default_rng(0)
    frames = [rng.integers(0, 255, size=(M, N), dtype="uint8") for _ in range(K)]
    data_uris = []
    for i, frame in enumerate(frames):
        filepath = Path(tmpdir) / f"frame{i:05}.tif"
        tifffile.imwrite(str(filepath), frame)
        data_uris.append(ensure_uri(str(filepath)))

    # Eager references, computed independently of the adapter's own logic.
    full = numpy.stack(frames).reshape(P, Q, R, M, N)
    # A cube whose value at (p, q, r, :, :) is the flat file index; slicing it
    # the same way tells us exactly which files a given slice depends on.
    idx_cube = numpy.broadcast_to(
        numpy.arange(K).reshape(P, Q, R, 1, 1), (P, Q, R, M, N)
    )

    struct = ArrayStructure(
        shape=(P, Q, R, M, N),
        chunks=((1,) * P, (1,) * Q, (1,) * R, (M,), (N,)),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
    )
    # `properties["chunks"]` carries the *pre-reshape* (stacked) chunk layout:
    # K single-file chunks along the stacking axis, then the per-file M, N dims.
    pre_reshape_chunks = [[1] * K, [M], [N]]

    adapter = in_memory(readable_storage=[tmpdir])
    await adapter.startup()
    try:
        await adapter.create_node(
            key="seq",
            structure_family="array",
            metadata={},
            data_sources=[
                DataSource(
                    structure_family="array",
                    mimetype="multipart/related;type=image/tiff",
                    structure=asdict(struct),
                    parameters={},
                    properties={"chunks": pre_reshape_chunks},
                    management="external",
                    assets=[
                        Asset(
                            parameter="data_uris",
                            num=i,
                            data_uri=data_uri,
                            is_directory=False,
                        )
                        for i, data_uri in enumerate(data_uris)
                    ],
                )
            ],
        )
        x = await adapter.lookup_adapter(["seq"])

        reference = full[slice_input]
        expected_indices = set(numpy.unique(idx_cube[slice_input]).tolist())

        # The lazy adapter must be used (not a fallback) and must resolve
        # exactly the files this slice touches -- no more, no fewer.
        lazy = await x._get_lazy_adapter(slice=slice_input)
        assert lazy is not None
        resolved_indices = {
            i for i, uri in enumerate(lazy.filepaths._data_uris) if uri is not None
        }
        assert resolved_indices == expected_indices

        # And a full read through the catalog returns the correct data.
        result = await x.read(slice=slice_input)
        numpy.testing.assert_array_equal(result, reference)
    finally:
        await adapter.shutdown()


@pytest.mark.parametrize("num_offset", [1, 5])
@pytest.mark.asyncio
async def test_lazy_sequence_tolerates_num_offset(tmpdir, num_offset):
    """The lazy path maps stacking rank to `num - offset`, so a contiguous run of
    `num`s starting at any offset (e.g. 1-based numbering) resolves the same
    files as 0-based numbering. Only the offset shifts; the dense stacking order
    (and thus the eager reference) is unchanged.
    """
    P, Q, R, M, N = _RESHAPE_P, _RESHAPE_Q, _RESHAPE_R, _RESHAPE_M, _RESHAPE_N
    K = P * Q * R

    rng = numpy.random.default_rng(1)
    frames = [rng.integers(0, 255, size=(M, N), dtype="uint8") for _ in range(K)]
    data_uris = []
    for i, frame in enumerate(frames):
        filepath = Path(tmpdir) / f"frame{i:05}.tif"
        tifffile.imwrite(str(filepath), frame)
        data_uris.append(ensure_uri(str(filepath)))

    full = numpy.stack(frames).reshape(P, Q, R, M, N)
    idx_cube = numpy.broadcast_to(
        numpy.arange(K).reshape(P, Q, R, 1, 1), (P, Q, R, M, N)
    )
    struct = ArrayStructure(
        shape=(P, Q, R, M, N),
        chunks=((1,) * P, (1,) * Q, (1,) * R, (M,), (N,)),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
    )
    pre_reshape_chunks = [[1] * K, [M], [N]]
    slice_input = (slice(0, 2), 1, slice(0, 4, 2))

    adapter = in_memory(readable_storage=[tmpdir])
    await adapter.startup()
    try:
        await adapter.create_node(
            key="seq",
            structure_family="array",
            metadata={},
            data_sources=[
                DataSource(
                    structure_family="array",
                    mimetype="multipart/related;type=image/tiff",
                    structure=asdict(struct),
                    parameters={},
                    properties={"chunks": pre_reshape_chunks},
                    management="external",
                    assets=[
                        # `num` starts at `num_offset` (e.g. 1-based), but the
                        # assets are still in dense stacking order.
                        Asset(
                            parameter="data_uris",
                            num=i + num_offset,
                            data_uri=data_uri,
                            is_directory=False,
                        )
                        for i, data_uri in enumerate(data_uris)
                    ],
                )
            ],
        )
        x = await adapter.lookup_adapter(["seq"])

        reference = full[slice_input]
        # Expected 0-based stacking ranks (offset already accounted for).
        expected_indices = set(numpy.unique(idx_cube[slice_input]).tolist())

        lazy = await x._get_lazy_adapter(slice=slice_input)
        assert lazy is not None
        resolved_indices = {
            i for i, uri in enumerate(lazy.filepaths._data_uris) if uri is not None
        }
        assert resolved_indices == expected_indices

        result = await x.read(slice=slice_input)
        numpy.testing.assert_array_equal(result, reference)
    finally:
        await adapter.shutdown()


@pytest.mark.parametrize(
    "slice_input",
    [
        Ellipsis,  # full read -> every file
        1,  # single leftmost index
        (slice(0, 2), 1, slice(0, 4, 2)),  # strided along the R (stacking) axis
        (slice(None), slice(1, 3), slice(0, 4, 2), Ellipsis, slice(0, 4)),
        (Ellipsis, 0, 0, 0),
        (0, Ellipsis, slice(0, 3)),
        (slice(0, 2), slice(0, 3), slice(1, 4, 2)),
        (slice(0, 1), slice(0, 2), slice(0, 2), slice(0, 2), slice(0, 3)),
    ],
)
@pytest.mark.asyncio
async def test_lazy_reshaped_hdf5_opens_only_touched_files(tmpdir, slice_input):
    """The lazy per-frame path opens exactly the HDF5 files that a reshaped
    slice needs -- no specs pass, no metadata open -- and reads back data
    identical to the eager reference.

    Each file stores one M x N dataset; the true concatenated shape is (K*M, N)
    but the structure declares (P, Q, R, M, N). The file count is the number of
    assets (K), and the leading (P, Q, R) structure dimensions map onto them.
    """
    from unittest.mock import patch

    import tiled.adapters.hdf5
    from tiled.ndslice import NDSlice

    h5py = pytest.importorskip("h5py")
    P, Q, R, M, N = _RESHAPE_P, _RESHAPE_Q, _RESHAPE_R, _RESHAPE_M, _RESHAPE_N
    K = P * Q * R

    # Distinct data per file so any mis-selection surfaces as a value mismatch.
    rng = numpy.random.default_rng(2)
    frames = [rng.integers(0, 255, size=(M, N), dtype="uint8") for _ in range(K)]
    data_uris = []
    filepaths = []
    for i, frame in enumerate(frames):
        filepath = Path(tmpdir) / f"frame{i:05}.h5"
        with h5py.File(filepath, "w") as f:
            f.create_dataset("a/b", data=frame)
        filepaths.append(filepath)
        data_uris.append(ensure_uri(str(filepath)))

    # Eager references, computed independently of the adapter's own logic.
    full = numpy.stack(frames).reshape(P, Q, R, M, N)
    idx_cube = numpy.broadcast_to(
        numpy.arange(K).reshape(P, Q, R, 1, 1), (P, Q, R, M, N)
    )

    struct = ArrayStructure(
        shape=(P, Q, R, M, N),
        chunks=((1,) * P, (1,) * Q, (1,) * R, (M,), (N,)),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
    )
    # `properties["chunks"]` carries the *pre-reshape* (stacked) chunk layout:
    # one whole-file chunk of M rows per file along the concatenation axis, then
    # the shared trailing N dim. len(chunks[0]) == K files; sum == K*M rows.
    pre_reshape_chunks = [[M] * K, [N]]

    adapter = in_memory(readable_storage=[tmpdir])
    await adapter.startup()
    try:
        await adapter.create_node(
            key="ds",
            structure_family="array",
            metadata={},
            data_sources=[
                DataSource(
                    structure_family="array",
                    mimetype="application/x-hdf5",
                    structure=asdict(struct),
                    parameters={"dataset": "a/b"},
                    properties={"chunks": pre_reshape_chunks},
                    management="external",
                    assets=[
                        Asset(
                            parameter="data_uris",
                            num=i,
                            data_uri=data_uri,
                            is_directory=False,
                        )
                        for i, data_uri in enumerate(data_uris)
                    ],
                )
            ],
        )
        x = await adapter.lookup_adapter(["ds"])

        reference = full[slice_input]
        expected_indices = set(numpy.unique(idx_cube[slice_input]).tolist())
        expected_names = {filepaths[i].name for i in expected_indices}

        # The lazy adapter must be used (not a fallback).
        lazy = await x._get_lazy_adapter(slice=slice_input)
        assert lazy is not None

        # Reading through the catalog opens exactly the files that this slice
        # touches, each exactly once: no specs pass and no metadata open. (The
        # server always presents a slice as an NDSlice.)
        with patch(
            "tiled.adapters.hdf5.h5open", wraps=tiled.adapters.hdf5.h5open
        ) as mock_h5open:
            result = await x.read(slice=NDSlice(slice_input))
            opened = [Path(call.args[0]).name for call in mock_h5open.call_args_list]

        assert set(opened) == expected_names
        assert len(opened) == len(expected_names)
        numpy.testing.assert_array_equal(result, reference)
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_lazy_reshaped_hdf5_read_block(tmpdir):
    """`read_block` takes the same lazy path. The catalog resolves the whole
    leading-axis slab a block sits in, but a block addresses a single grid cell
    (one file), and Dask culls the rest -- so exactly that one file is opened and
    the data matches the eager reference.
    """
    from unittest.mock import patch

    import tiled.adapters.hdf5
    from tiled.ndslice import NDBlock

    h5py = pytest.importorskip("h5py")
    P, Q, R, M, N = _RESHAPE_P, _RESHAPE_Q, _RESHAPE_R, _RESHAPE_M, _RESHAPE_N
    K = P * Q * R

    rng = numpy.random.default_rng(3)
    frames = [rng.integers(0, 255, size=(M, N), dtype="uint8") for _ in range(K)]
    data_uris = []
    filepaths = []
    for i, frame in enumerate(frames):
        filepath = Path(tmpdir) / f"frame{i:05}.h5"
        with h5py.File(filepath, "w") as f:
            f.create_dataset("a/b", data=frame)
        filepaths.append(filepath)
        data_uris.append(ensure_uri(str(filepath)))

    full = numpy.stack(frames).reshape(P, Q, R, M, N)
    struct = ArrayStructure(
        shape=(P, Q, R, M, N),
        chunks=((1,) * P, (1,) * Q, (1,) * R, (M,), (N,)),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
    )
    pre_reshape_chunks = [[M] * K, [N]]

    adapter = in_memory(readable_storage=[tmpdir])
    await adapter.startup()
    try:
        await adapter.create_node(
            key="ds",
            structure_family="array",
            metadata={},
            data_sources=[
                DataSource(
                    structure_family="array",
                    mimetype="application/x-hdf5",
                    structure=asdict(struct),
                    parameters={"dataset": "a/b"},
                    properties={"chunks": pre_reshape_chunks},
                    management="external",
                    assets=[
                        Asset(
                            parameter="data_uris",
                            num=i,
                            data_uri=data_uri,
                            is_directory=False,
                        )
                        for i, data_uri in enumerate(data_uris)
                    ],
                )
            ],
        )
        x = await adapter.lookup_adapter(["ds"])

        # `read_block` only accepts blocks whose non-leading indices are 0; such a
        # block addresses grid cell (p, 0, 0), i.e. file index p * Q * R.
        for p in range(P):
            block = NDBlock(p, 0, 0, 0, 0)
            reference = full[p : p + 1, 0:1, 0:1]  # noqa: E203
            expected_name = filepaths[p * Q * R].name

            lazy = await x._get_lazy_adapter(block=block)
            assert lazy is not None
            with patch(
                "tiled.adapters.hdf5.h5open", wraps=tiled.adapters.hdf5.h5open
            ) as mock_h5open:
                result = await x.read_block(block)
                opened = [
                    Path(call.args[0]).name for call in mock_h5open.call_args_list
                ]
            # Over-resolved to the leading slab, but Dask culls to the one cell.
            assert opened == [expected_name]
            numpy.testing.assert_array_equal(result, reference)
    finally:
        await adapter.shutdown()


@pytest.mark.parametrize(
    "slice_input",
    [
        0,  # single event -> one file
        2,  # single event -> one file
        (1, slice(0, 2)),  # event 1, first two frames -> still one file
        (slice(1, 3),),  # two events -> two files
    ],
)
@pytest.mark.asyncio
async def test_lazy_reshaped_hdf5_multichunk_files(tmpdir, slice_input):
    """A file that holds many native chunks along the concatenation axis still
    takes the lazy path.

    Each file stores an (F, H, W) dataset chunked one frame at a time, so its
    native tiling is (1, H, W): `properties["chunks"][0]` has K*F ones, far more
    than the K files. The structure declares (K, F, H, W). The file count is the
    number of assets (K), NOT `len(chunks[0])` (== K*F), and the leading axis
    maps onto the files -- so reading one event opens exactly one file. (This is
    the shape of a per-frame-chunked detector stream.)
    """
    from unittest.mock import patch

    import tiled.adapters.hdf5
    from tiled.ndslice import NDSlice

    h5py = pytest.importorskip("h5py")
    K, F, H, W = 3, 4, 2, 3

    rng = numpy.random.default_rng(5)
    files = [rng.integers(0, 255, size=(F, H, W), dtype="uint8") for _ in range(K)]
    data_uris = []
    filepaths = []
    for i, file_data in enumerate(files):
        filepath = Path(tmpdir) / f"file{i:05}.h5"
        with h5py.File(filepath, "w") as f:
            # One frame per native chunk: many native chunks per file.
            f.create_dataset("a/b", data=file_data, chunks=(1, H, W))
        filepaths.append(filepath)
        data_uris.append(ensure_uri(str(filepath)))

    # Eager reference and a cube labelling every element by its source file.
    full = numpy.stack(files)  # (K, F, H, W)
    idx_cube = numpy.broadcast_to(numpy.arange(K).reshape(K, 1, 1, 1), (K, F, H, W))

    struct = ArrayStructure(
        shape=(K, F, H, W),
        chunks=((1,) * K, (F,), (H,), (W,)),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
    )
    # Pre-reshape (native) tiling: one frame per chunk over the K*F concatenated
    # frames, then the shared trailing H, W dims. len(chunks[0]) == K*F != K.
    pre_reshape_chunks = [[1] * (K * F), [H], [W]]

    adapter = in_memory(readable_storage=[tmpdir])
    await adapter.startup()
    try:
        await adapter.create_node(
            key="ds",
            structure_family="array",
            metadata={},
            data_sources=[
                DataSource(
                    structure_family="array",
                    mimetype="application/x-hdf5",
                    structure=asdict(struct),
                    parameters={"dataset": "a/b"},
                    properties={"chunks": pre_reshape_chunks},
                    management="external",
                    assets=[
                        Asset(
                            parameter="data_uris",
                            num=i,
                            data_uri=data_uri,
                            is_directory=False,
                        )
                        for i, data_uri in enumerate(data_uris)
                    ],
                )
            ],
        )
        x = await adapter.lookup_adapter(["ds"])

        reference = full[slice_input]
        expected_indices = set(numpy.unique(idx_cube[slice_input]).tolist())
        expected_names = {filepaths[i].name for i in expected_indices}

        # The lazy adapter must be used (not a fallback).
        lazy = await x._get_lazy_adapter(slice=slice_input)
        assert lazy is not None

        with patch(
            "tiled.adapters.hdf5.h5open", wraps=tiled.adapters.hdf5.h5open
        ) as mock_h5open:
            result = await x.read(slice=NDSlice(slice_input))
            opened = [Path(call.args[0]).name for call in mock_h5open.call_args_list]

        assert set(opened) == expected_names
        assert len(opened) == len(expected_names)
        numpy.testing.assert_array_equal(result, reference)
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_lazy_hdf5_falls_back_when_not_file_aligned(tmpdir):
    """When the reshape is not file-boundary-aligned the lazy path bows out
    (`_get_lazy_adapter` returns None) and the eager build still reads correctly.
    """
    from tiled.ndslice import NDSlice

    h5py = pytest.importorskip("h5py")
    # K files of M rows each; a structure whose leading dims never multiply to
    # the file count K, so no whole-file split exists.
    K, M, N = 6, 5, 3
    rng = numpy.random.default_rng(4)
    frames = [rng.integers(0, 255, size=(M, N), dtype="uint8") for _ in range(K)]
    data_uris = []
    for i, frame in enumerate(frames):
        filepath = Path(tmpdir) / f"frame{i:05}.h5"
        with h5py.File(filepath, "w") as f:
            f.create_dataset("a/b", data=frame)
        data_uris.append(ensure_uri(str(filepath)))

    # true concatenated shape (K*M, N) == (30, 3); reshape to (10, 3, 3) splits
    # the file boundary (10 > K at the first axis), so it is not file-aligned.
    full = numpy.concatenate(frames, axis=0).reshape(10, 3, N)
    struct = ArrayStructure(
        shape=(10, 3, N),
        chunks=((1,) * 10, (3,), (N,)),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
    )
    pre_reshape_chunks = [[M] * K, [N]]

    adapter = in_memory(readable_storage=[tmpdir])
    await adapter.startup()
    try:
        await adapter.create_node(
            key="ds",
            structure_family="array",
            metadata={},
            data_sources=[
                DataSource(
                    structure_family="array",
                    mimetype="application/x-hdf5",
                    structure=asdict(struct),
                    parameters={"dataset": "a/b"},
                    properties={"chunks": pre_reshape_chunks},
                    management="external",
                    assets=[
                        Asset(
                            parameter="data_uris",
                            num=i,
                            data_uri=data_uri,
                            is_directory=False,
                        )
                        for i, data_uri in enumerate(data_uris)
                    ],
                )
            ],
        )
        x = await adapter.lookup_adapter(["ds"])

        # Not file-aligned -> lazy path declines, eager path still correct.
        assert await x._get_lazy_adapter(slice=(slice(0, 2),)) is None
        result = await x.read(slice=NDSlice((slice(0, 2),)))
        numpy.testing.assert_array_equal(result, full[0:2])
    finally:
        await adapter.shutdown()


# Genuinely non-uniform files: K files hold DIFFERENT numbers of frames, stored
# flat-concatenated along axis 0. The native tiling (one frame per chunk) can not
# reveal the file boundaries -- only the optional per-asset `properties["extents"]`
# does. `extents[i]` is file i's extent along axis 0; `sum(extents) == shape[0]`.
_NONUNIFORM_EXTENTS = [2, 5, 3]


@pytest.mark.parametrize(
    "slice_input,expected_indices",
    [
        (0, {0}),  # first row -> first file
        (6, {1}),  # a row deep inside the second file
        ((slice(0, 2),), {0}),  # exactly the first file's rows
        ((slice(1, 4),), {0, 1}),  # straddles the first boundary
        ((slice(6, 10),), {1, 2}),  # straddles the second boundary
        (Ellipsis, {0, 1, 2}),  # full read -> every file
    ],
)
@pytest.mark.asyncio
async def test_lazy_hdf5_nonuniform_extents_property(
    tmpdir, slice_input, expected_indices
):
    """`properties["extents"]` enables the lazy path for non-uniform files stored
    flat, mapping an axis-0 slice to exactly the files it touches via a cumulative
    sum of the per-file extents.

    Each file holds a different frame count and is chunked one frame at a time, so
    the native tiling (`len(chunks[0]) == sum(extents)`) can not delimit the files;
    `extents` supplies the boundaries the catalog can not otherwise infer.
    """
    from unittest.mock import patch

    import tiled.adapters.hdf5
    from tiled.ndslice import NDSlice

    h5py = pytest.importorskip("h5py")
    extents = _NONUNIFORM_EXTENTS
    K, H, W = len(extents), 2, 3
    total = sum(extents)

    rng = numpy.random.default_rng(6)
    files = [rng.integers(0, 255, size=(f, H, W), dtype="uint8") for f in extents]
    data_uris = []
    filepaths = []
    for i, file_data in enumerate(files):
        filepath = Path(tmpdir) / f"file{i:05}.h5"
        with h5py.File(filepath, "w") as f:
            f.create_dataset("a/b", data=file_data, chunks=(1, H, W))
        filepaths.append(filepath)
        data_uris.append(ensure_uri(str(filepath)))

    # Flat concatenation along axis 0; a cube labelling each row by its file.
    full = numpy.concatenate(files, axis=0)  # (total, H, W)
    idx_cube = numpy.broadcast_to(
        numpy.repeat(numpy.arange(K), extents).reshape(total, 1, 1), (total, H, W)
    )

    struct = ArrayStructure(
        shape=(total, H, W),
        chunks=((1,) * total, (H,), (W,)),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
    )
    # Native (per-frame) tiling can not delimit files; `extents` supplies it.
    pre_reshape_chunks = [[1] * total, [H], [W]]

    adapter = in_memory(readable_storage=[tmpdir])
    await adapter.startup()
    try:
        await adapter.create_node(
            key="ds",
            structure_family="array",
            metadata={},
            data_sources=[
                DataSource(
                    structure_family="array",
                    mimetype="application/x-hdf5",
                    structure=asdict(struct),
                    parameters={"dataset": "a/b"},
                    properties={"chunks": pre_reshape_chunks, "extents": extents},
                    management="external",
                    assets=[
                        Asset(
                            parameter="data_uris",
                            num=i,
                            data_uri=data_uri,
                            is_directory=False,
                        )
                        for i, data_uri in enumerate(data_uris)
                    ],
                )
            ],
        )
        x = await adapter.lookup_adapter(["ds"])

        reference = full[slice_input]
        assert set(numpy.unique(idx_cube[slice_input]).tolist()) == expected_indices
        expected_names = {filepaths[i].name for i in expected_indices}

        # The lazy adapter is used (not a fallback).
        lazy = await x._get_lazy_adapter(slice=slice_input)
        assert lazy is not None

        with patch(
            "tiled.adapters.hdf5.h5open", wraps=tiled.adapters.hdf5.h5open
        ) as mock_h5open:
            result = await x.read(slice=NDSlice(slice_input))
            opened = [Path(call.args[0]).name for call in mock_h5open.call_args_list]

        assert set(opened) == expected_names
        assert len(opened) == len(expected_names)
        numpy.testing.assert_array_equal(result, reference)
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_lazy_hdf5_nonuniform_requires_extents(tmpdir):
    """Without `properties["extents"]` the same non-uniform flat layout has no way
    to locate file boundaries, so the lazy path bows out and the eager build
    still reads correctly.
    """
    from tiled.ndslice import NDSlice

    h5py = pytest.importorskip("h5py")
    extents = _NONUNIFORM_EXTENTS
    H, W = 2, 3
    total = sum(extents)

    rng = numpy.random.default_rng(7)
    files = [rng.integers(0, 255, size=(f, H, W), dtype="uint8") for f in extents]
    data_uris = []
    for i, file_data in enumerate(files):
        filepath = Path(tmpdir) / f"file{i:05}.h5"
        with h5py.File(filepath, "w") as f:
            f.create_dataset("a/b", data=file_data, chunks=(1, H, W))
        data_uris.append(ensure_uri(str(filepath)))

    full = numpy.concatenate(files, axis=0)
    struct = ArrayStructure(
        shape=(total, H, W),
        chunks=((1,) * total, (H,), (W,)),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
    )
    # No `extents`, and the native tiling can not delimit the K files.
    pre_reshape_chunks = [[1] * total, [H], [W]]

    adapter = in_memory(readable_storage=[tmpdir])
    await adapter.startup()
    try:
        await adapter.create_node(
            key="ds",
            structure_family="array",
            metadata={},
            data_sources=[
                DataSource(
                    structure_family="array",
                    mimetype="application/x-hdf5",
                    structure=asdict(struct),
                    parameters={"dataset": "a/b"},
                    properties={"chunks": pre_reshape_chunks},
                    management="external",
                    assets=[
                        Asset(
                            parameter="data_uris",
                            num=i,
                            data_uri=data_uri,
                            is_directory=False,
                        )
                        for i, data_uri in enumerate(data_uris)
                    ],
                )
            ],
        )
        x = await adapter.lookup_adapter(["ds"])

        assert await x._get_lazy_adapter(slice=(slice(0, 2),)) is None
        result = await x.read(slice=NDSlice((slice(0, 2),)))
        numpy.testing.assert_array_equal(result, full[0:2])
    finally:
        await adapter.shutdown()


@pytest.mark.parametrize("transform", [{"slice": ":,0,:"}, {"squeeze": True}])
def test_file_indices_for_slice_declines_under_transform(transform):
    """A `slice`/`squeeze` adapter parameter reshapes each file when the served
    array is built, so the served axis 0 no longer maps to whole backing files.
    `file_indices_for_slice` must decline (return None) so the catalog skips the
    lazy path -- even when the structure alone (one leading chunk per file) would
    otherwise let the files be located from `chunks[0]`.
    """
    from tiled.adapters.hdf5 import HDF5ArrayAdapter

    # One leading chunk per file: without a transform the files ARE locatable.
    struct = ArrayStructure(
        shape=(3, 2, 3),
        chunks=((1, 1, 1), (2,), (3,)),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
    )
    sl = (slice(0, 2),)

    # Baseline: no transform -> the lazy path can locate the touched files.
    assert HDF5ArrayAdapter.file_indices_for_slice(struct, 3, sl) == (0, 1)
    assert HDF5ArrayAdapter.file_indices_for_slice(struct, 3, sl, parameters={}) == (
        0,
        1,
    )
    # A slice/squeeze transform is present -> decline regardless of the layout.
    assert (
        HDF5ArrayAdapter.file_indices_for_slice(struct, 3, sl, parameters=transform)
        is None
    )


@pytest.mark.asyncio
async def test_lazy_hdf5_declines_under_slice_squeeze(tmpdir):
    """A data source carrying a `slice`/`squeeze` parameter must not take the lazy
    path even when its structure would otherwise locate files: those transforms
    reshape each file on read, breaking the served-axis-0 -> file mapping. The
    catalog threads the data source `parameters` to the adapter, which declines,
    and the eager build still reads correctly.
    """
    from tiled.ndslice import NDSlice

    h5py = pytest.importorskip("h5py")
    # Uniform files, one leading chunk each: the structure alone locates files
    # (len(chunks[0]) == n_files), so only the transform can hold back the lazy path.
    K, M, N = 4, 2, 3
    rng = numpy.random.default_rng(11)
    files = [rng.integers(0, 255, size=(1, M, N), dtype="uint8") for _ in range(K)]
    data_uris = []
    for i, file_data in enumerate(files):
        filepath = Path(tmpdir) / f"file{i:05}.h5"
        with h5py.File(filepath, "w") as f:
            f.create_dataset("a/b", data=file_data, chunks=(1, M, N))
        data_uris.append(ensure_uri(str(filepath)))

    full = numpy.concatenate(files, axis=0)  # (K, M, N)
    struct = ArrayStructure(
        shape=(K, M, N),
        chunks=((1,) * K, (M,), (N,)),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
    )

    async def _make_node(adapter, key, parameters):
        await adapter.create_node(
            key=key,
            structure_family="array",
            metadata={},
            data_sources=[
                DataSource(
                    structure_family="array",
                    mimetype="application/x-hdf5",
                    structure=asdict(struct),
                    parameters=parameters,
                    properties={"chunks": [[M] * K, [M], [N]]},
                    management="external",
                    assets=[
                        Asset(
                            parameter="data_uris",
                            num=i,
                            data_uri=data_uri,
                            is_directory=False,
                        )
                        for i, data_uri in enumerate(data_uris)
                    ],
                )
            ],
        )

    adapter = in_memory(readable_storage=[tmpdir])
    await adapter.startup()
    try:
        # Control: no transform -> the structure lets the lazy path engage.
        await _make_node(adapter, "plain", {"dataset": "a/b"})
        ctrl = await adapter.lookup_adapter(["plain"])
        assert await ctrl._get_lazy_adapter(slice=(slice(0, 2),)) is not None

        # A squeeze parameter (a no-op on this non-singleton array) is enough to
        # hold back the lazy path; the eager read still returns the whole array.
        await _make_node(adapter, "squeezed", {"dataset": "a/b", "squeeze": True})
        x = await adapter.lookup_adapter(["squeezed"])
        assert await x._get_lazy_adapter(slice=(slice(0, 2),)) is None
        result = await x.read(slice=NDSlice((slice(0, 2),)))
        numpy.testing.assert_array_equal(result, full[0:2])
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_write_table_external_direct(a, tmpdir):
    df = pandas.DataFrame(numpy.ones((5, 3)), columns=list("abc"))
    filepath = str(tmpdir / "file.csv")
    data_uri = ensure_uri(filepath)
    df.to_csv(filepath, index=False)
    dfa = CSVAdapter.from_uris(data_uri)
    structure = asdict(dfa.structure())
    await a.create_node(
        key="x",
        structure_family=StructureFamily.table,
        metadata={},
        data_sources=[
            DataSource(
                structure_family="table",
                mimetype="text/csv",
                structure=structure,
                parameters={},
                management="external",
                assets=[
                    Asset(
                        parameter="data_uris",
                        num=0,
                        data_uri=data_uri,
                        is_directory=False,
                    )
                ],
            )
        ],
    )
    x = await a.lookup_adapter(["x"])
    pandas.testing.assert_frame_equal(await x.read(), df)


@pytest.mark.asyncio
async def test_write_array_internal_direct(a, tmpdir):
    from tiled.media_type_registration import default_deserialization_registry

    arr = numpy.ones((5, 3))
    ad = ArrayAdapter.from_array(arr)
    structure = ad.structure()
    await a.create_node(
        key="x",
        structure_family="array",
        metadata={},
        data_sources=[
            DataSource(
                structure_family="array",
                structure=structure,
                management="writable",
            )
        ],
    )
    x = await a.lookup_adapter(["x"])

    media_type = "application/octet-stream"
    body = arr.tobytes()
    deserializer = default_deserialization_registry.dispatch("array", media_type)
    await x.write(media_type, deserializer, x, body)

    val = await x.read()
    assert numpy.array_equal(val, arr)


def test_write_array_internal_via_client(client):
    expected = numpy.array([1, 3, 7])
    x = client.write_array(expected)
    actual = x.read()
    assert numpy.array_equal(actual, expected)

    y = client.write_array(dask.array.from_array(expected, chunks=((1, 1, 1),)))
    actual = y.read()
    assert numpy.array_equal(actual, expected)


def test_write_table_internal_via_client(client):
    expected = pandas.DataFrame(numpy.ones((5, 3)), columns=list("abc"))
    x = client.write_table(expected)
    actual = x.read()
    pandas.testing.assert_frame_equal(actual, expected)

    # y = client.write_array(dask.array.from_array(expected, chunks=((1, 1, 1),)))
    # actual = y.read()
    # assert numpy.array_equal(actual, expected)
    # pandas.testing.assert_frame_equal(actual, expected)


def test_write_xarray_dataset(client):
    ds = xarray.Dataset(
        {"temp": (["time"], numpy.array([101, 102, 103]))},
        coords={"time": (["time"], numpy.array([1, 2, 3]))},
    )
    dsc = write_xarray_dataset(client, ds, key="test_xarray_dataset")
    assert set(dsc) == {"temp", "time"}
    # smoke test
    dsc["temp"][:]
    dsc["time"][:]
    dsc.read()


@pytest.mark.asyncio
async def test_delete_catalog_tree(tmpdir):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.
    tree = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        a = client.create_container("a")
        b = a.create_container("b")
        b.write_array([1, 2, 3])
        b.write_array([4, 5, 6])
        c = b.create_container("c")
        d = c.create_container("d")
        d.write_array([7, 8, 9])

        nodes_before_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_before_delete) == 7 + 1  # +1 for the root node
        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 3
        assets_before_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_before_delete) == 3

        with pytest.raises(Conflicts, match="Cannot delete a node that is not empty."):
            await tree.delete()

        with pytest.raises(WouldDeleteData):
            await tree.delete(recursive=True)  # external_only=True by default
        with pytest.raises(WouldDeleteData):
            await tree.delete(recursive=True, external_only=True)
        await tree.delete(recursive=True, external_only=False)

        nodes_after_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_after_delete) == 0 + 1  # the root node that should remain
        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 0
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 0


@pytest.mark.asyncio
async def test_delete_contents(tmpdir):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.
    tree = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        # a has children b1 and b2, which each contain arrays
        a = client.create_container("a")
        b1 = a.create_container("b1")
        b1.write_array([1, 2, 3], key="test_1")
        b1.write_array([4, 5, 6], key="test_2")
        b1.write_array([7, 8, 9], key="test_3")
        b2 = a.create_container("b2")
        b2.write_array([10, 11, 12], key="test_4")
        b2.write_array([13, 14, 15], key="test_5")
        a.create_container("b3")  # empty container

        assert set(client) == {"a"}
        assert set(client["a"]) == {"b1", "b2", "b3"}
        assert set(client["a"]["b1"]) == {"test_1", "test_2", "test_3"}
        assert set(client["a"]["b2"]) == {"test_4", "test_5"}

        # Check the database state before deletion
        nodes_before_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_before_delete) == 9 + 1
        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 5
        assets_before_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_before_delete) == 5

        # Trying to delete a non-empty node without recursive=True should raise
        with pytest.raises(
            ClientError, match="Cannot delete a node that is not empty."
        ):
            client["a"].delete_contents(["b1"], recursive=False, external_only=True)

        # Trying to delete internal data with external_only=True should raise
        with pytest.raises(
            ClientError, match="Some items in this tree are internally managed."
        ):
            client["a"].delete_contents(["b1"], recursive=True, external_only=True)

        # Delete arrays from b1 (as a scalar and as a list), and then b1 itself
        b1.delete_contents("test_1", external_only=False)
        assert set(client["a"]["b1"].keys()) == {"test_2", "test_3"}
        b1.delete_contents(["test_2", "test_3"], external_only=False)
        assert set(client["a"]["b1"].keys()) == set()
        client["a"].delete_contents(["b1"], recursive=False, external_only=True)
        assert set(client["a"]) == {"b2", "b3"}

        # Delete all contents of a, including the non-empty b2 and the empty b3
        client["a"].delete_contents(external_only=False, recursive=True)
        assert set(client["a"]) == set()

        # Check the database state; only a and the root node should remain.
        nodes_after_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_after_delete) == 1 + 1
        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 0
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 0


@pytest.mark.asyncio
async def test_delete_with_external_nodes(tmpdir):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.
    (tmpdir / "readable").mkdir()
    (tmpdir / "writable").mkdir()
    tree = in_memory(
        readable_storage=[str(tmpdir / "readable")],
        writable_storage={"filesystem": str(tmpdir / "writable")},
    )

    # Create some external data to register
    for i in range(1, 5):
        with open(tmpdir / "readable" / f"test_{i}.csv", "w") as file:
            file.write(
                """a, b, c
                    1, 2, 3
                    4, 5, 6
                """
            )
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        # a has children b1 and b2, which each contain arrays
        a = client.create_container("a")
        b1 = a.create_container("b1")
        await register(b1, tmpdir / "readable" / "test_1.csv")
        await register(b1, tmpdir / "readable" / "test_2.csv")
        b2 = a.create_container("b2")
        await register(b2, tmpdir / "readable" / "test_3.csv")
        await register(b2, tmpdir / "readable" / "test_4.csv")

        assert list(client) == ["a"]
        assert list(client["a"]) == ["b1", "b2"]
        assert list(client["a"]["b1"]) == ["test_1", "test_2"]
        assert list(client["a"]["b2"]) == ["test_3", "test_4"]

        nodes_before_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_before_delete) == 7 + 1  # +1 for the root node
        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 4
        assets_before_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_before_delete) == 4

        # Delete all children of b1, and b1 itself.
        client["a"].delete_contents("b1", recursive=True)

        assert list(client) == ["a"]
        assert list(client["a"]) == ["b2"]
        assert list(client["a"]["b2"]) == ["test_3", "test_4"]  # not affected
        nodes_after_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_after_delete) == 4 + 1  # +1 for the root node
        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 2
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 2


@pytest.mark.asyncio
async def test_delete_sql_assets(sql_storage_uri):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.

    tree = in_memory(writable_storage={"sql": sql_storage_uri})
    storage = cast(SQLStorage, get_storage(parse_storage(sql_storage_uri).uri))

    # Create some tables to write
    table_1 = pyarrow.Table.from_pydict({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    table_2 = pyarrow.Table.from_pydict({"c": [4, 5, 6], "d": ["7", "8", "9"]})

    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        # a has children b1 and b2, which each contain arrays
        a = client.create_container("a")
        b1 = a.create_container("b1")
        t1 = b1.create_appendable_table(schema=table_1.schema, key="table_1")
        t1.append_partition(0, table_1)
        t1.append_partition(0, table_1)
        t2 = b1.create_appendable_table(schema=table_2.schema, key="table_2")
        t2.append_partition(0, table_2)
        assert t1.read() is not None
        assert t2.read() is not None

        # Check the SQL storage directly
        t1_table_name = t1.data_sources()[0].parameters["table_name"]
        t1_dataset_id = t1.data_sources()[0].parameters["dataset_id"]
        t2_table_name = t2.data_sources()[0].parameters["table_name"]
        t2_dataset_id = t2.data_sources()[0].parameters["dataset_id"]
        with closing(storage.connect()) as conn:
            assert sql_table_exists(conn, storage.dialect, t1_table_name)
            assert sql_table_exists(conn, storage.dialect, t2_table_name)
            with conn.cursor() as cursor:
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t1_table_name}" '
                    f"WHERE _dataset_id = {t1_dataset_id:d};",
                )
                assert cursor.fetchone()[0] == 6
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t2_table_name}" '
                    f"WHERE _dataset_id = {t2_dataset_id:d};",
                )
                assert cursor.fetchone()[0] == 3

        # Add another table to b2 -- a copy of table_1 with the same schema
        b2 = a.create_container("b2")
        t1c = b2.create_appendable_table(schema=table_1.schema, key="table_1_copy")
        t1c.append_partition(0, table_1)
        assert t1c.read() is not None

        # Check the catalog state before deletion
        assert list(client) == ["a"]
        assert list(client["a"]) == ["b1", "b2"]
        assert list(client["a"]["b1"]) == ["table_1", "table_2"]
        assert list(client["a"]["b2"]) == ["table_1_copy"]

        # Check the number of nodes, data sources, and assets
        nodes_before_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_before_delete) == 6 + 1  # +1 for the root node
        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 3
        assets_before_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_before_delete) == 1  # single sql asset

        # Check the SQL storage directly
        t1c_table_name = t1c.data_sources()[0].parameters["table_name"]
        t1c_dataset_id = t1c.data_sources()[0].parameters["dataset_id"]
        assert t1c_table_name == t1_table_name
        assert t1c_dataset_id != t1_dataset_id
        with closing(storage.connect()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t1_table_name}";',
                )
                assert cursor.fetchone()[0] == 9
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t2_table_name}";',
                )
                assert cursor.fetchone()[0] == 3
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t1c_table_name}" '
                    f"WHERE _dataset_id = {t1c_dataset_id:d};",
                )
                assert cursor.fetchone()[0] == 3

        # Delete all children of b1 (tables t1 and t2), but not b1 itself.
        client["a"]["b1"].delete_contents(
            client["a"]["b1"].keys(), recursive=True, external_only=False
        )
        with closing(storage.connect()) as conn:
            with conn.cursor() as cursor:
                assert sql_table_exists(conn, storage.dialect, t1_table_name)
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{t1_table_name}";',
                )
                assert cursor.fetchone()[0] == 3  # 6 rows deleted
                # Entire t2 deleted
                assert not sql_table_exists(conn, storage.dialect, t2_table_name)

        assert list(client) == ["a"]
        assert list(client["a"]) == ["b1", "b2"]
        assert (
            list(client["a"]["b1"]) == []
        )  # children deleted (2 nodes, 2 data sources, 0 assets)
        assert list(client["a"]["b2"]) == ["table_1_copy"]  # not affected
        nodes_after_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_after_delete) == 4 + 1  # +1 for the root node
        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 1
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 1

        # Close and dispose the SQL storage
    storage.dispose()


# ---------------------------------------------------------------------------
# Hypothesis: pagination completeness
# ---------------------------------------------------------------------------
# The adapter is started once per module (per dialect) and reused across all
# hypothesis examples. Each example creates a fresh child container, runs its
# traversal inside it, then deletes it, keeping the shared DB clean.
#
# asyncio: hypothesis does not support async test functions, so we keep a
# single event loop alive for the lifetime of the fixture and drive all
# coroutines with loop.run_until_complete().
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sqlite_adapter_for_hypothesis(tmp_path_factory):
    loop = asyncio.new_event_loop()
    tmpdir = tmp_path_factory.mktemp("hyp_sqlite")
    adapter = in_memory(writable_storage=str(tmpdir))
    loop.run_until_complete(adapter.startup())
    yield adapter, loop
    loop.run_until_complete(adapter.shutdown())
    loop.close()


@pytest.fixture(scope="module")
def postgresql_adapter_for_hypothesis(tmp_path_factory):
    from tiled.catalog import from_uri

    from .conftest import TILED_TEST_POSTGRESQL_URI, temp_postgres

    if not TILED_TEST_POSTGRESQL_URI:
        pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
    loop = asyncio.new_event_loop()
    tmpdir = tmp_path_factory.mktemp("hyp_pg")

    async def _setup():
        ctx = temp_postgres(TILED_TEST_POSTGRESQL_URI)
        uri = await ctx.__aenter__()
        adapter = from_uri(uri, writable_storage=str(tmpdir), init_if_not_exists=True)
        await adapter.startup()
        return adapter, ctx

    adapter, ctx = loop.run_until_complete(_setup())
    yield adapter, loop

    async def _teardown():
        await adapter.shutdown()
        await ctx.__aexit__(None, None, None)

    loop.run_until_complete(_teardown())
    loop.close()


@pytest.fixture(
    scope="module",
    params=["sqlite_adapter_for_hypothesis", "postgresql_adapter_for_hypothesis"],
)
def catalog_adapter_for_hypothesis(request):
    yield request.getfixturevalue(request.param)


@given(
    n_items=st.integers(min_value=0, max_value=50),
    page_size=st.integers(min_value=1, max_value=20),
    direction=st.sampled_from([1, -1]),
)
@settings(deadline=None, max_examples=50)
def test_cursor_pagination_completeness(
    catalog_adapter_for_hypothesis, n_items, page_size, direction
):
    """Traversing all cursor pages yields every item exactly once, in order.

    Properties checked:
    - total count matches n_items
    - no duplicates across pages
    - order matches insertion order (ASC) or reverse insertion order (DESC),
      since id is strictly monotonic
    """
    adapter, loop = catalog_adapter_for_hypothesis

    async def run():
        # Use a unique container key so parallel/sequential examples don't collide.
        import uuid

        container_key = uuid.uuid4().hex
        await adapter.create_node(
            key=container_key,
            metadata={},
            structure_family=StructureFamily.container,
            specs=[],
        )
        child = await adapter.lookup_adapter([container_key])
        try:
            keys_inserted = [str(i) for i in range(n_items)]
            for key in keys_inserted:
                await child.create_node(
                    key=key,
                    metadata={},
                    structure_family=StructureFamily.container,
                    specs=[],
                )
            sorted_child = child.sort([("", direction)])
            collected = []
            cursor = None
            while True:
                page, next_cursor = await sorted_child.keys_page(
                    cursor=cursor, limit=page_size
                )
                collected.extend(page)
                if next_cursor is None:
                    break
                cursor = next_cursor
            return keys_inserted, collected
        finally:
            child_adapter = await adapter.lookup_adapter([container_key])
            await child_adapter.delete(recursive=True, external_only=False)

    keys_inserted, collected = loop.run_until_complete(run())

    assert len(collected) == n_items, "total item count must match"
    assert len(set(collected)) == len(collected), "no duplicates across pages"
    if direction == 1:
        assert collected == keys_inserted, "ASC traversal must match insertion order"
    else:
        assert collected == list(
            reversed(keys_inserted)
        ), "DESC traversal must match reverse insertion order"


@pytest.mark.asyncio
async def test_delete_external_asset_registered_twice(tmpdir):
    # Do not use client fixture here.
    # The Context must be opened inside the test or we run into
    # event loop crossing issues with the Postgres test.
    tree = in_memory(readable_storage=[str(tmpdir)])
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        for i in range(1, 4):
            with open(tmpdir / f"test_{i}.csv", "w") as file:
                file.write(
                    """a, b, c
                        1, 2, 3
                        4, 5, 6
                    """
                )
        # a has children b1 and b2, which each contain arrays
        a = client.create_container("a")
        b1 = a.create_container("b1")
        await register(b1, tmpdir / "test_1.csv")
        await register(b1, tmpdir / "test_2.csv")
        b2 = a.create_container("b2")
        await register(b2, tmpdir / "test_1.csv")
        await register(b2, tmpdir / "test_3.csv")

        # test_1.csv is registered in both b1 and b2
        assert client["a"]["b1"]["test_1"].read() is not None
        assert client["a"]["b2"]["test_1"].read() is not None

        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 4
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 3  # shared by two data sources

        a.delete_contents("b2", recursive=True)

        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 2
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 2

        # The asset in b1 should still be accessible.
        client["a"]["b1"]["test_1"].read()

        a.delete_contents("b1", recursive=True)
        data_sources_after_second_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_second_delete) == 0
        assets_after_second_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_after_second_delete) == 0


@pytest.mark.parametrize(
    "assets",
    [
        [
            Asset(
                data_uri="file://localhost/test1",
                is_directory=False,
                parameter="filepath",
                num=None,
            ),
            Asset(
                data_uri="file://localhost/test2",
                is_directory=False,
                parameter="filepath",
                num=1,
            ),
        ],
        [
            Asset(
                data_uri="file://localhost/test1",
                is_directory=False,
                parameter="filepath",
                num=1,
            ),
            Asset(
                data_uri="file://localhost/test2",
                is_directory=False,
                parameter="filepath",
                num=None,
            ),
        ],
        [
            Asset(
                data_uri="file://localhost/test1",
                is_directory=False,
                parameter="filepath",
                num=None,
            ),
            Asset(
                data_uri="file://localhost/test2",
                is_directory=False,
                parameter="filepath",
                num=None,
            ),
        ],
        [
            Asset(
                data_uri="file://localhost/test1",
                is_directory=False,
                parameter="filepath",
                num=1,
            ),
            Asset(
                data_uri="file://localhost/test2",
                is_directory=False,
                parameter="filepath",
                num=1,
            ),
        ],
    ],
    ids=[
        "null-then-int",
        "int-then-null",
        "duplicate-null",
        "duplicate-int",
    ],
)
@pytest.mark.asyncio
async def test_constraints_on_parameter_and_num(a, assets):
    "Test constraints enforced by database on 'parameter' and 'num'."
    arr_adapter = ArrayAdapter.from_array([1, 2, 3])
    with pytest.raises(
        (
            sqlalchemy.exc.IntegrityError,  # SQLite
            sqlalchemy.exc.DBAPIError,  # PostgreSQL
        )
    ):
        await a.create_node(
            key="test",
            structure_family=arr_adapter.structure_family,
            metadata=dict(arr_adapter.metadata()),
            specs=arr_adapter.specs,
            data_sources=[
                DataSource(
                    structure_family=arr_adapter.structure_family,
                    mimetype="text/csv",
                    structure=arr_adapter.structure(),
                    parameters={},
                    management=Management.external,
                    assets=assets,
                )
            ],
        )


@pytest.mark.asyncio
async def test_init_db_logging(sqlite_or_postgres_uri, tmpdir, caplog):
    config = {
        "database": {
            "uri": "sqlite://",  # in-memory
        },
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": str(tmpdir / "data"),
                    "init_if_not_exists": True,
                },
            },
        ],
    }
    # Issue 721 notes that the logging of the subprocess that creates
    # a database logs normal things to error. This test looks at the log
    # and fails if an error log happens. This could catch anything that is
    # an error during the app build.
    import logging

    with caplog.at_level(logging.INFO):
        app = build_app_from_config(config)
        for record in caplog.records:
            assert record.levelname != "ERROR", f"Error found creating app {record.msg}"
        assert app


@pytest.mark.parametrize(
    "exact_count_limit, expected_lower_bound", [(None, 10), (5, 6), (-1, 10)]
)
@pytest.mark.asyncio
async def test_container_length(
    sqlite_or_postgres_uri, exact_count_limit, expected_lower_bound
):
    config = {
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "init_if_not_exists": True,
                },
            },
        ],
    }
    if exact_count_limit is not None:
        config["exact_count_limit"] = exact_count_limit

    app = build_app_from_config(config)

    # Turn off autovacuum in Postgres (just in case)
    # Create a separate engine to avoid interfeing with the running loop
    if sqlite_or_postgres_uri.startswith("postgresql"):
        engine = create_async_engine(
            ensure_specified_sql_driver(sqlite_or_postgres_uri)
        )
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    ALTER TABLE nodes
                    SET (autovacuum_enabled = false,
                        autovacuum_analyze_threshold = 0);
                """
                )
            )

    with Context.from_app(app) as context:
        client = from_context(context)

        # Create a container with some nested nodes
        a = client.create_container("a")
        for i in range(10):
            b = a.create_container(key=f"node_{i}")
            b.create_container(key=f"subnode_{i}")

        # Before analyzing the table, the length should be thresholded
        len_from_metadata = client["a"].item["attributes"]["structure"]["count"]
        assert len_from_metadata == expected_lower_bound

        # Analyze the table to get update pg_statistics
        if sqlite_or_postgres_uri.startswith("postgresql"):
            async with engine.connect() as conn:
                conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
                await conn.execute(text("VACUUM ANALYZE nodes;"))
            await engine.dispose()

        # After analyzing, the length should be updated (at least be approximate)
        len_from_metadata = client["a"].item["attributes"]["structure"]["count"]
        assert len_from_metadata <= 10

        # len() returns the exact count
        assert len(client["a"]) == 10


@pytest.mark.parametrize(
    "desired, expected",
    [((None, None, None, None), (5, 5, 10, 10)), ((7, 11, 13, 17), (7, 11, 13, 17))],
)
def test_pooling_config(sqlite_or_postgres_uri, sql_storage_uri, desired, expected):
    config = {
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": sql_storage_uri,
                    "init_if_not_exists": True,
                },
            },
        ]
    }
    pool_config = {
        "catalog_pool_size": desired[0],
        "storage_pool_size": desired[1],
        "catalog_max_overflow": desired[2],
        "storage_max_overflow": desired[3],
    }
    if any(v is not None for v in desired):
        config.update(pool_config)

    app = build_app_from_config(config)

    # Check the catalog pool
    catalog_pool = app.state.root_tree.context.engine.pool
    assert isinstance(catalog_pool, AsyncAdaptedQueuePool)
    assert catalog_pool.size() == expected[0]
    assert catalog_pool._max_overflow == expected[2]

    # Check the storage pool
    storage = get_storage(ensure_uri(sanitize_uri(sql_storage_uri)[0]))
    storage: SQLStorage = cast(SQLStorage, storage)

    if sql_storage_uri.startswith("duckdb"):
        # DuckDB does not support pooling
        assert isinstance(storage._connection_pool, StaticPool)
        assert storage.pool_size == 1
        assert storage.max_overflow == 0
    else:
        assert isinstance(storage._connection_pool, QueuePool)
        assert storage.pool_size == expected[1]
        assert storage.max_overflow == expected[3]
        assert storage._connection_pool.size() == expected[1]
        assert storage._connection_pool._max_overflow == expected[3]

    storage.dispose()
