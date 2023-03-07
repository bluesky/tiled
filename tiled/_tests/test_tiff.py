from pathlib import Path

import numpy
import pytest
import tifffile as tf

from ..adapters.mapping import MapAdapter
from ..adapters.tiff import TiffAdapter, TiffSequenceAdapter, subdirectory_handler
from ..client import Context, from_context
from ..server.app import build_app, build_app_from_config

COLOR_SHAPE = (11, 17, 3)


@pytest.fixture(scope="module")
def client(tmpdir_module):
    data = numpy.random.random((5, 7))
    sequence_directory = Path(tmpdir_module, "sequence")
    sequence_directory.mkdir()
    for i in range(3):
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


def test_tiff_sequence_with_directory_walker(tmpdir):
    """
    directory/
      sequence/  # should trigger subdirectory_handler
      other_stuff/  # should not
      other_file1.csv  # should not
      other_file2.csv  # should not
    """
    data = numpy.random.random((100, 101))
    # This directory should be detected as a TIFF sequence.
    Path(tmpdir, "sequence").mkdir()
    for i in range(10):
        tf.imwrite(Path(tmpdir, "sequence", f"image{i:05}.tif"), data)
    # This directory should *not* be detected as a TIFF sequence.
    Path(tmpdir, "other_stuff").mkdir()
    tf.imwrite(Path(tmpdir, "other_stuff", "image.tif"), data)
    for target in ["other_stuff/stuff.csv", "other_file1.csv", "other_file2.csv"]:
        with open(Path(tmpdir, target), "w") as file:
            file.write(
                """
a,b,c
1,2,3
"""
            )
    config = {
        "trees": [
            {
                "tree": "files",
                "path": "/",
                "args": {
                    "directory": tmpdir,
                    "subdirectory_handler": subdirectory_handler,
                },
            }
        ]
    }
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        client = from_context(context)
        # This whole directory of files is one dataset.
        assert client["sequence"].read().shape == (10, 100, 101)
        # This directory has one dataset per file, in the normal fashion.
        client["other_stuff"]["image"].read()
        assert list(client["other_stuff"]["stuff"].read().columns) == ["a", "b", "c"]
        assert list(client["other_file1"].read().columns) == ["a", "b", "c"]
        assert list(client["other_file2"].read().columns) == ["a", "b", "c"]


def test_rgb(client):
    "Test an RGB TIFF."
    arr = client["color"].read()
    assert arr.shape == COLOR_SHAPE
