from pathlib import Path

import numpy
import pytest
import tifffile as tf

from ..adapters.mapping import MapAdapter
from ..adapters.tiff import TiffAdapter, TiffSequenceAdapter
from ..catalog import in_memory
from ..client import Context, from_context
from ..client.register import IMG_SEQUENCE_EMPTY_NAME_ROOT, register
from ..server.app import build_app
from ..structures.array import ArrayStructure, BuiltinDtype
from ..utils import ensure_uri

COLOR_SHAPE = (11, 17, 3)
rng = numpy.random.default_rng(12345)


@pytest.fixture(scope="module")
def client(tmpdir_module):
    sequence_directory = Path(tmpdir_module, "sequence")
    sequence_directory.mkdir()
    filepaths = []
    for i in range(3):
        data = rng.integers(0, 255, size=(5, 7, 4), dtype="uint8")
        filepath = sequence_directory / f"temp{i:05}.tif"
        tf.imwrite(filepath, data)
        filepaths.append(filepath)
    color_data = rng.integers(0, 255, size=COLOR_SHAPE, dtype="uint8")
    path = Path(tmpdir_module, "color.tif")
    tf.imwrite(path, color_data)
    tree = MapAdapter(
        {
            "color": TiffAdapter(ensure_uri(path)),
            "sequence": TiffSequenceAdapter.from_uris(
                *[ensure_uri(filepath) for filepath in filepaths]
            ),
            "5d_sequence": TiffSequenceAdapter(
                [ensure_uri(filepath) for filepath in filepaths],
                structure=ArrayStructure(
                    shape=(3, 1, 5, 7, 4),
                    chunks=((1, 1, 1), (1,), (5,), (7,), (4,)),
                    data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
                ),
            ),
        }
    )
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        yield client


@pytest.mark.parametrize(
    "slice_input, correct_shape",
    [
        (None, (3, 5, 7, 4)),
        (0, (5, 7, 4)),
        (slice(0, 3, 2), (2, 5, 7, 4)),
        ((1, slice(0, 3), slice(0, 3)), (3, 3, 4)),
        ((slice(0, 3), slice(0, 3), slice(0, 3)), (3, 3, 3, 4)),
        ((..., 0, 0, 0), (3,)),
        ((0, slice(0, 1), slice(0, 2), ...), (1, 2, 4)),
        ((0, ..., slice(0, 2)), (5, 7, 2)),
        ((..., slice(0, 1)), (3, 5, 7, 1)),
    ],
)
def test_tiff_sequence(client, slice_input, correct_shape):
    arr = client["sequence"].read(slice=slice_input)
    assert arr.shape == correct_shape


@pytest.mark.filterwarnings("ignore: Forcefully reshaping ")
@pytest.mark.parametrize(
    "slice_input, correct_shape",
    [
        (None, (3, 1, 5, 7, 4)),
        (..., (3, 1, 5, 7, 4)),
        ((), (3, 1, 5, 7, 4)),
        (0, (1, 5, 7, 4)),
        (slice(0, 3, 2), (2, 1, 5, 7, 4)),
        ((1, slice(0, 10), slice(0, 3), slice(0, 3)), (1, 3, 3, 4)),
        ((slice(0, 3), 0, slice(0, 3), slice(0, 3)), (3, 3, 3, 4)),
        ((..., 0, 0, 0, 0), (3,)),
        ((0, slice(0, 1), slice(0, 1), slice(0, 2), ...), (1, 1, 2, 4)),
        ((0, ..., slice(0, 2)), (1, 5, 7, 2)),
        ((..., slice(0, 1)), (3, 1, 5, 7, 1)),
    ],
)
def test_forced_reshaping(client, slice_input, correct_shape):
    arr = client["5d_sequence"].read(slice=slice_input)
    assert arr.shape == correct_shape


@pytest.mark.parametrize("block_input, correct_shape", [((0, 0, 0, 0), (1, 5, 7, 4))])
def test_tiff_sequence_block(client, block_input, correct_shape):
    arr = client["sequence"].read_block(block_input)
    assert arr.shape == correct_shape


@pytest.mark.asyncio
async def test_tiff_sequence_order(tmpdir):
    """
    directory/
      00001.tif
      00002.tif
      ...
      00010.tif
    """
    data = numpy.ones((4, 5))
    num_files = 10
    for i in range(num_files):
        tf.imwrite(Path(tmpdir / f"image{i:05}.tif"), data * i)

    adapter = in_memory(readable_storage=[tmpdir])
    with Context.from_app(build_app(adapter)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        for i in range(num_files):
            numpy.testing.assert_equal(client["image"][i], data * i)


@pytest.mark.asyncio
async def test_tiff_sequence_with_directory_walker(tmpdir):
    """
    directory/
      00001.tif
      00002.tif
      ...
      00010.tif
      single_image.tif
      image00001.tif
      image00002.tif
      ...
      image00010.tif
      other_image00001.tif
      other_image00002.tif
      ...
      other_image00010.tif
      other_image2_00001.tif
      other_image2_00002.tif
      ...
      other_image2_00010.tif
      other_file1.csv
      other_file2.csv
      stuff.csv
    """
    data = numpy.random.random((3, 5))
    for i in range(10):
        tf.imwrite(Path(tmpdir / f"image{i:05}.tif"), data)
        tf.imwrite(Path(tmpdir / f"other_image{i:05}.tif"), data)
        tf.imwrite(Path(tmpdir / f"{i:05}.tif"), data)
        tf.imwrite(Path(tmpdir / f"other_image2_{i:05}.tif"), data)
    tf.imwrite(Path(tmpdir / "single_image.tif"), data)
    for target in ["stuff.csv", "other_file1.csv", "other_file2.csv"]:
        with open(Path(tmpdir / target), "w") as file:
            file.write(
                """
a,b,c
1,2,3
"""
            )
    adapter = in_memory(readable_storage=[tmpdir])
    with Context.from_app(build_app(adapter)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        # Single image is its own node.
        assert client["single_image"].shape == (3, 5)
        # Each sequence is grouped into a node.
        assert client[IMG_SEQUENCE_EMPTY_NAME_ROOT].shape == (10, 3, 5)
        assert client["image"].shape == (10, 3, 5)
        assert client["other_image"].shape == (10, 3, 5)
        assert client["other_image2_"].shape == (10, 3, 5)
        # The sequence grouping digit-only files appears with a uuid
        named_keys = [
            "single_image",
            "image",
            "other_image",
            "other_image2_",
            "other_file1",
            "other_file2",
            "stuff",
        ]
        no_name_keys = [key for key in client.keys() if key not in named_keys]
        # There is only a single one of this type
        assert len(no_name_keys) == 1
        assert client[no_name_keys[0]].shape == (10, 3, 5)
        # Other files are single nodes.
        assert client["stuff"].columns == ["a", "b", "c"]
        assert client["other_file1"].columns == ["a", "b", "c"]
        assert client["other_file2"].columns == ["a", "b", "c"]


def test_rgb(client):
    "Test an RGB TIFF."
    arr = client["color"].read()
    assert arr.shape == COLOR_SHAPE


def test_tiff_sequence_cache(client):
    from numpy.testing import assert_raises

    # The two requests go through the same method in the server (read_block) to
    # call the same object
    indexed_array = client["sequence"][0]
    read_array = client["sequence"].read(0)

    # Using a different index to confirm that the previous cache doesn't affect the new array
    other_read_array = client["sequence"].read(1)

    numpy.testing.assert_equal(indexed_array, read_array)
    assert_raises(
        AssertionError, numpy.testing.assert_equal, read_array, other_read_array
    )
