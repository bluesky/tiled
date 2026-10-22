"""Unit tests for the array protocol layer (`tiled.client._protocol.array`).

These tests exercise fetch planners, write-request builders, data-source
builders, and response parsers as plain functions, with no server and no I/O.
"""


import math

import numpy
import pytest

from tiled.client._protocol.array import (
    ArrayReadRecipe,
    data_source_from_array,
    draft_request_patch_array_full,
    draft_request_write_array_block,
    draft_request_write_array_full,
)
from tiled.client.utils import patch_conflict_message
from tiled.ndslice import NDBlock, NDSlice
from tiled.structures.array import ArrayStructure, BuiltinDtype
from tiled.structures.core import StructureFamily, structure_from_json

FULL_LINK = "http://localhost:8000/api/v1/array/full/a/b"
# The server provides the block link with the `block` query parameter as a
# URL template, one positional placeholder per dimension.
BLOCK_LINK = "http://localhost:8000/api/v1/array/block/a/b?block={0},{1}"
OCTET_STREAM_MIMETYPE = "application/octet-stream"


def make_structure(shape=(3, 4), chunks=((2, 1), (2, 2)), dtype="int32"):
    return ArrayStructure(
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype(dtype)),
        chunks=chunks,
        shape=shape,
    )


@pytest.mark.parametrize(
    "array_slice, expected_shape, expected_params",
    [
        # An empty slice fetches the whole array and adds no `slice` param.
        (NDSlice(), (3, 4), {"expected_shape": "3,4"}),
        # A non-empty slice is expanded over the full shape on the wire.
        (
            NDSlice(slice(0, 1), slice(0, 2)),
            (1, 2),
            {"expected_shape": "1,2", "slice": "0:1:1,0:2:1"},
        ),
        # Integer indices drop dimensions from the expected shape.
        (NDSlice(0), (4,), {"expected_shape": "4", "slice": "0,0:4:1"}),
    ],
)
def test_for_full_slice_params(array_slice, expected_shape, expected_params):
    recipe = ArrayReadRecipe.for_full(
        FULL_LINK, make_structure(), array_slice, bytesize_limit=10**9
    )
    (draft,) = recipe.drafts.values()
    assert draft.method == "GET"
    assert draft.url == FULL_LINK
    assert draft.params == expected_params
    assert draft.headers == {"Accept": OCTET_STREAM_MIMETYPE}
    assert draft.content is None
    assert recipe.dtype == numpy.dtype("int32")
    assert recipe.shape == expected_shape


def test_for_full_scalar():
    # A 0-dimensional array has an empty shape, sent as "scalar".
    recipe = ArrayReadRecipe.for_full(
        FULL_LINK,
        make_structure(shape=(), chunks=()),
        NDSlice(),
        bytesize_limit=10**9,
    )
    (draft,) = recipe.drafts.values()
    assert draft.params == {"expected_shape": "scalar"}
    assert recipe.shape == ()


@pytest.mark.parametrize(
    "shape, chunks, array_slice, expected_shape",
    [
        ((0, 3), ((0,), (3,)), NDSlice(), (0, 3)),
        ((3, 4), ((3,), (4,)), NDSlice(slice(2, 2)), (0, 4)),
        ((0, 4), ((0,), (2, 2)), NDSlice(), (0, 4)),
    ],
)
def test_for_full_zero_dim_short_circuit(shape, chunks, array_slice, expected_shape):
    # An array with a 0-length dimension never contains data, so no request
    # is drafted; the result is assembled (empty) locally.
    recipe = ArrayReadRecipe.for_full(
        FULL_LINK, make_structure(shape, chunks), array_slice, bytesize_limit=10**9
    )
    assert recipe.drafts == {}
    assert recipe.shape == expected_shape
    assert recipe.assemble({}).shape == expected_shape


@pytest.mark.parametrize(
    "structure_kwargs, block, block_slice, expected_params, expected_shape",
    [
        # The baked `block` template in the link's query is overridden by
        # the actual block index.
        ({}, NDBlock(0, 1), None, {"block": "0,1", "expected_shape": "2,2"}, (2, 2)),
        # An Ellipsis block expands to slices over the full chunk grid.
        (
            {},
            NDBlock(...),
            None,
            {"block": "0:2:1,0:2:1", "expected_shape": "3,4"},
            (3, 4),
        ),
        # A slice within the block narrows the expected shape.
        (
            {},
            NDBlock(0, 1),
            NDSlice(slice(0, 1), slice(0, 2)),
            {"block": "0,1", "expected_shape": "1,2", "slice": "0:1,0:2"},
            (1, 2),
        ),
        # A 0-dimensional array has one (index-less) block and an empty
        # shape, sent as "scalar".
        (
            {"shape": (), "chunks": ()},
            NDBlock(),
            None,
            {"block": "", "expected_shape": "scalar"},
            (),
        ),
    ],
    ids=["block", "ellipsis", "slice", "scalar"],
)
def test_for_block(
    structure_kwargs, block, block_slice, expected_params, expected_shape
):
    recipe = ArrayReadRecipe.for_block(
        BLOCK_LINK, make_structure(**structure_kwargs), block, block_slice
    )
    ((index, draft),) = recipe.drafts.items()
    assert draft.method == "GET"
    assert draft.url == BLOCK_LINK
    assert draft.params == expected_params
    assert draft.headers == {"Accept": OCTET_STREAM_MIMETYPE}
    assert recipe.shape == expected_shape
    assert recipe.shape_of_draft(index) == expected_shape


def test_for_block_zero_dim_short_circuit():
    block_slice = NDSlice(slice(0, 0))
    recipe = ArrayReadRecipe.for_block(
        BLOCK_LINK, make_structure(), NDBlock(0, 1), block_slice
    )
    assert recipe.drafts == {}
    assert recipe.shape == (0, 2)


@pytest.mark.parametrize("shape", [(6,), (2, 3), ()])
def test_parse_array_response_round_trip(shape):
    array = numpy.arange(numpy.prod(shape, dtype=int), dtype="int32").reshape(shape)
    parsed = ArrayReadRecipe.parse(array.tobytes(), array.dtype, shape)
    assert parsed.shape == shape
    assert numpy.array_equal(parsed, array)


@pytest.mark.parametrize("shape", [(0,), (0, 3), (3, 0, 2)])
def test_empty(shape):
    structure = make_structure(shape=shape, chunks=tuple((s,) for s in shape))
    recipe = ArrayReadRecipe.for_full(
        FULL_LINK, structure, NDSlice(), bytesize_limit=10**9
    )
    assert not recipe.drafts
    array = recipe.empty()
    assert array.shape == shape
    assert array.dtype == structure.data_type.to_numpy_dtype()
    assert array.size == 0


def test_for_full_single_fetch():
    # A read below the byte-size limit is one fetch covering the whole
    # (sliced) array, tiled as a single chunk.
    structure = make_structure(shape=(4,), chunks=((2, 2),), dtype="float64")
    recipe = ArrayReadRecipe.for_full(
        FULL_LINK, structure, NDSlice(), bytesize_limit=1000
    )
    assert recipe.shape == (4,)
    assert recipe.tiling == ((4,),)
    assert list(recipe.drafts) == [(0,)]
    assert recipe.drafts[(0,)].params == {"expected_shape": "4"}


def test_for_full_splits_along_chunk_boundaries():
    # A 4-element read with a 2-element size limit splits at the chunk
    # boundary into two fetches.
    structure = make_structure(shape=(4,), chunks=((2, 2),), dtype="float64")
    recipe = ArrayReadRecipe.for_full(
        FULL_LINK, structure, NDSlice(), bytesize_limit=16
    )
    assert list(recipe.drafts) == [(0,), (1,)]
    assert recipe.tiling == ((2, 2),)
    assert recipe.shape_of_draft((0,)) == (2,)
    assert recipe.shape_of_draft((1,)) == (2,)
    assert recipe.drafts[(1,)].params["slice"] == "2:4:1"


@pytest.mark.parametrize(
    "shape, chunks, bytesize_limit, expected_num_tiles",
    [
        ((4, 3), ((2, 2), (3,)), 6 * 4, 2),  # split: one request per two rows
        ((4, 3), ((2, 2), (3,)), 10**9, 1),  # small enough for one request
        ((0, 3), ((0,), (3,)), 10**9, 0),  # zero-size: no requests at all
    ],
)
def test_read_recipe_assemble(shape, chunks, bytesize_limit, expected_num_tiles):
    # Sub-results keyed by tile index are placed at the offsets implied by
    # the recipe tiling. The degenerate tilings need no stitching: a single
    # sub-result is returned as-is (no copy) and a zero-size result is
    # constructed locally from no sub-results.
    structure = make_structure(shape=shape, chunks=chunks, dtype="int32")
    recipe = ArrayReadRecipe.for_full(
        FULL_LINK, structure, NDSlice(), bytesize_limit=bytesize_limit
    )
    expected = numpy.arange(math.prod(shape), dtype="int32").reshape(shape)
    results = {
        index: expected[
            tuple(
                slice(sum(recipe.tiling[dim][:i]), sum(recipe.tiling[dim][: i + 1]))
                for dim, i in enumerate(index)
            )
        ]
        for index in recipe.drafts
    }
    assert len(results) == expected_num_tiles
    assembled = recipe.assemble(results)
    numpy.testing.assert_array_equal(assembled, expected)
    assert assembled.dtype == expected.dtype
    if expected_num_tiles == 1:
        (sub_result,) = results.values()
        assert assembled is sub_result


def test_draft_request_write_array_full():
    array = numpy.arange(6, dtype="float64")
    draft = draft_request_write_array_full(FULL_LINK, array)
    assert draft.method == "PUT"
    assert draft.url == FULL_LINK
    assert draft.params == {}
    assert draft.headers == {"Content-Type": OCTET_STREAM_MIMETYPE}
    assert draft.content == array.tobytes()


@pytest.mark.parametrize(
    "build",
    [
        lambda array, persist: draft_request_write_array_full(
            FULL_LINK, array, persist=persist
        ),
        lambda array, persist: draft_request_write_array_block(
            BLOCK_LINK, (0, 0), array, persist=persist
        ),
        lambda array, persist: draft_request_patch_array_full(
            FULL_LINK, array, offset=0, persist=persist
        ),
    ],
    ids=["write", "write_block", "patch"],
)
def test_persist_param_only_when_non_default(build):
    array = numpy.arange(6, dtype="float64")
    assert "persist" not in build(array, True).params
    assert build(array, False).params["persist"] is False


def test_draft_request_write_array_full_coerces_array_like():
    draft = draft_request_write_array_full(FULL_LINK, [1, 2, 3])
    assert draft.content == numpy.asarray([1, 2, 3]).tobytes()


def test_draft_request_write_array_block():
    array = numpy.ones((2, 2), dtype="float64")
    draft = draft_request_write_array_block(BLOCK_LINK, (1, 0), array)
    # The block index fills the URL template and is folded back into params.
    assert draft.method == "PUT"
    assert draft.url == BLOCK_LINK.format(1, 0)
    assert draft.params == {"block": ["1,0"]}
    assert draft.headers == {"Content-Type": OCTET_STREAM_MIMETYPE}
    assert draft.content == array.tobytes()


def test_draft_request_write_array_block_with_slice():
    array = numpy.ones((2,), dtype="float64")
    draft = draft_request_write_array_block(
        BLOCK_LINK, (0, 0), array, slice=(slice(0, 2), 1)
    )
    assert draft.params == {"block": ["0,0"], "slice": ":2,1"}


def test_draft_request_patch_array_full():
    array = numpy.zeros((1, 2, 2), dtype="float64")
    draft = draft_request_patch_array_full(FULL_LINK, array, offset=(3,), extend=True)
    assert draft.method == "PATCH"
    assert draft.url == FULL_LINK
    assert draft.params == {"offset": "3", "shape": "1,2,2", "extend": True}
    assert draft.headers == {"Content-Type": OCTET_STREAM_MIMETYPE}
    assert draft.content == array.tobytes()


def test_draft_request_patch_array_full_integer_offset():
    array = numpy.zeros((2,), dtype="float64")
    draft = draft_request_patch_array_full(FULL_LINK, array, offset=3)
    assert draft.params == {"offset": "3", "shape": "2", "extend": False}


def test_draft_request_patch_array_full_makes_body_contiguous():
    array = numpy.arange(6, dtype="int32").reshape(2, 3).T  # not C-contiguous
    draft = draft_request_patch_array_full(FULL_LINK, array, offset=(0, 0))
    assert draft.content == numpy.ascontiguousarray(array).tobytes()
    assert draft.params["shape"] == "3,2"


@pytest.mark.parametrize(
    "body, expected",
    [
        # The server phrases conflicts in terms of URL query parameters;
        # the client rephrases them in terms of Python keyword arguments.
        (
            {"detail": "Use ?extend=true to extend array dimension."},
            "Pass keyword argument extend=true to extend array dimension.",
        ),
        ({}, "Array parameters conflict."),
    ],
)
def test_patch_conflict_message(body, expected):
    assert patch_conflict_message(body) == expected


def test_parse_patch_response():
    body = {
        "data_type": {
            "endianness": "little",
            "kind": "f",
            "itemsize": 8,
            "dt_units": None,
        },
        "chunks": [[3, 1], [2, 2]],
        "shape": [4, 4],
        "dims": None,
        "resizable": False,
    }
    structure = structure_from_json(StructureFamily.array, body)
    assert isinstance(structure, ArrayStructure)
    assert structure.shape == (4, 4)
    assert structure.chunks == ((3, 1), (2, 2))
    assert structure.data_type.to_numpy_dtype() == numpy.dtype("float64")


# Data source submitted when creating an array node


def test_data_source_from_array():
    data_source = data_source_from_array(
        numpy.arange(10, dtype="float64"), ["x"], 100_000_000
    )
    assert data_source.structure_family == StructureFamily.array
    structure = data_source.structure
    assert structure.shape == (10,)
    assert structure.chunks == ((10,),)  # small: a single chunk
    assert structure.dims == ["x"]
    assert structure.data_type.to_numpy_dtype() == numpy.dtype("float64")


def test_data_source_from_array_splits_large_upload():
    # Chunks are chosen such that each chunk is not too large to upload.
    data_source = data_source_from_array(
        numpy.zeros(1000, dtype="float64"), None, max_chunk_bytes=800
    )
    (axis_chunks,) = data_source.structure.chunks
    assert len(axis_chunks) > 1
    assert sum(axis_chunks) == 1000
    assert max(axis_chunks) * 8 <= 800


def test_data_source_from_array_respects_existing_chunks():
    import dask.array

    array = dask.array.zeros(4, dtype="uint8", chunks=(2,))
    data_source = data_source_from_array(array, None, 100_000_000)
    assert data_source.structure.chunks == ((2, 2),)
