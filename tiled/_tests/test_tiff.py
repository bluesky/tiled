from pathlib import Path

import numpy
import pytest
import tifffile as tf

from ..adapters.mapping import MapAdapter
from ..adapters.tiff import TiffAdapter, TiffSequenceAdapter
from ..catalog import in_memory
from ..catalog.register import register
from ..client import Context, from_context
from ..server.app import build_app

COLOR_SHAPE = (11, 17, 3)


@pytest.fixture(scope="module")
def client(tmpdir_module):
    sequence_directory = Path(tmpdir_module, "sequence")
    sequence_directory.mkdir()
    for i in range(3):
        data = numpy.random.random((5, 7))
        tf.imwrite(sequence_directory / f"temp{i:05}.tif", data)
    color_data = numpy.random.randint(0, 255, COLOR_SHAPE, dtype="uint8")
    path = Path(tmpdir_module, "color.tif")
    tf.imwrite(path, color_data)

    tree = MapAdapter(
        {
            "color": TiffAdapter(str(path)),
            "sequence": TiffSequenceAdapter(
                tf.TiffSequence(str(sequence_directory / "*.tif"))
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
        (None, (3, 5, 7)),
        (0, (5, 7)),
        (slice(0, 3, 2), (2, 5, 7)),
        ((1, slice(0, 3), slice(0, 3)), (3, 3)),
        ((slice(0, 3), slice(0, 3), slice(0, 3)), (3, 3, 3)),
    ],
)
def test_tiff_sequence(client, slice_input, correct_shape):
    arr = client["sequence"].read(slice=slice_input)
    assert arr.shape == correct_shape


@pytest.mark.parametrize("block_input, correct_shape", [((0, 0, 0), (1, 5, 7))])
def test_tiff_sequence_block(client, block_input, correct_shape):
    arr = client["sequence"].read_block(block_input)
    assert arr.shape == correct_shape


@pytest.mark.asyncio
async def test_tiff_sequence_with_directory_walker(tmpdir):
    """
    directory/
      single_image.tif
      image00001.tif
      image00002.tif
      ...
      image00010.tif
      other_image00001.tif
      other_image00002.tif
      ...
      other_image00010.tif
      other_file1.csv
      other_file2.csv
      stuff.csv
    """
    data = numpy.random.random((3, 5))
    for i in range(10):
        tf.imwrite(Path(tmpdir / f"image{i:05}.tif"), data)
        tf.imwrite(Path(tmpdir / f"other_image{i:05}.tif"), data)
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
        await register(adapter, tmpdir)
        client = from_context(context)
        # Single image is its own node.
        assert client["single_image"].shape == (3, 5)
        # Each sequence is grouped into a node.
        assert client["image"].shape == (10, 3, 5)
        assert client["other_image"].shape == (10, 3, 5)
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
