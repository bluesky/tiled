from pathlib import Path

import numpy
import pytest
from PIL import Image

from ..adapters.jpeg import JPEGAdapter, JPEGSequenceAdapter
from ..adapters.mapping import MapAdapter
from ..catalog import in_memory
from ..client import Context, from_context
from ..client.register import IMG_SEQUENCE_EMPTY_NAME_ROOT, register
from ..server.app import build_app
from ..utils import ensure_uri

COLOR_SHAPE = (11, 17, 3)


@pytest.fixture(scope="module")
def client(tmpdir_module):
    sequence_directory = Path(tmpdir_module, "sequence")
    sequence_directory.mkdir()
    filepaths = []
    for i in range(3):
        # JPEGs can only be 8 bit ints
        data = numpy.random.randint(0, 255, (5, 7), dtype="uint8")
        filepath = sequence_directory / f"temp{i:05}.jpeg"
        Image.fromarray(data).convert("L").save(filepath)
        filepaths.append(filepath)
    color_data = numpy.random.randint(0, 255, COLOR_SHAPE, dtype="uint8")
    path = Path(tmpdir_module, "color.jpeg")
    Image.fromarray(color_data).convert("RGB").save(path)

    tree = MapAdapter(
        {
            "color": JPEGAdapter.from_uris(ensure_uri(path)),
            "sequence": JPEGSequenceAdapter.from_uris(
                *[ensure_uri(filepath) for filepath in filepaths]
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
        ((..., 0, 0), (3,)),
        ((0, slice(0, 1), slice(0, 2), ...), (1, 2)),
        ((0, ..., slice(0, 2)), (5, 2)),
        ((..., slice(0, 1)), (3, 5, 1)),
    ],
)
def test_jpeg_sequence(client, slice_input, correct_shape):
    arr = client["sequence"].read(slice=slice_input)
    assert arr.shape == correct_shape


@pytest.mark.parametrize("block_input, correct_shape", [((0, 0, 0), (1, 5, 7))])
def test_jpeg_sequence_block(client, block_input, correct_shape):
    arr = client["sequence"].read_block(block_input)
    assert arr.shape == correct_shape


@pytest.mark.asyncio
async def test_jpeg_sequence_order(tmpdir):
    """
    directory/
      00001.jpeg
      00002.jpeg
      ...
      00010.jpeg
    """
    data = numpy.ones((4, 5))
    num_files = 10
    for i in range(num_files):
        Image.fromarray(data * i).convert("L").save(Path(tmpdir / f"image{i:05}.jpeg"))

    adapter = in_memory(readable_storage=[tmpdir])
    with Context.from_app(build_app(adapter)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        for i in range(num_files):
            numpy.testing.assert_equal(client["image"][i], data * i)


@pytest.mark.asyncio
async def test_jpeg_sequence_with_directory_walker(tmpdir):
    """
    directory/
      00001.jpeg
      00002.jpeg
      ...
      00010.jpeg
      single_image.jpeg
      image00001.jpeg
      image00002.jpeg
      ...
      image00010.jpeg
      other_image00001.jpeg
      other_image00002.jpeg
      ...
      other_image00010.jpeg
      other_image2_00001.jpeg
      other_image2_00002.jpeg
      ...
      other_image2_00010.jpeg
      other_file1.csv
      other_file2.csv
      stuff.csv
    """
    data = numpy.random.randint(0, 255, (3, 5), dtype="uint8")
    for i in range(10):
        Image.fromarray(data).convert("L").save(Path(tmpdir / f"image{i:05}.jpeg"))
        Image.fromarray(data).convert("L").save(
            Path(tmpdir / f"other_image{i:05}.jpeg")
        )
        Image.fromarray(data).convert("L").save(Path(tmpdir / f"{i:05}.jpeg"))
        Image.fromarray(data).convert("L").save(
            Path(tmpdir / f"other_image2_{i:05}.jpeg")
        )
    Image.fromarray(data).save(Path(tmpdir / "single_image.jpeg"))
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
    "Test an RGB JPEG."
    arr = client["color"].read()
    assert arr.shape == COLOR_SHAPE


def test_jpeg_sequence_cache(client):
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
