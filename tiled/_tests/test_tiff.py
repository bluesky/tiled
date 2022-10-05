from pathlib import Path

import numpy
import pytest
import tifffile as tf

from ..adapters.mapping import MapAdapter
from ..adapters.tiff import TiffAdapter, TiffSequenceAdapter, subdirectory_handler
from ..client import from_config, from_tree


@pytest.fixture
def directory(tmpdir):
    data = numpy.random.random((5, 7))
    for i in range(3):
        tf.imwrite(Path(tmpdir, f"temp{i:05}.tif"), data)
    return str(Path(tmpdir, "*.tif"))


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
def test_tiff_sequence(directory, slice_input, correct_shape):
    tree = MapAdapter({"A": TiffSequenceAdapter(tf.TiffSequence(directory))})
    client = from_tree(tree)
    arr = client["A"].read(slice=slice_input)
    assert arr.shape == correct_shape


@pytest.mark.parametrize("block_input, correct_shape", [((0, 0, 0), (1, 5, 7))])
def test_tiff_sequence_block(directory, block_input, correct_shape):
    tree = MapAdapter({"A": TiffSequenceAdapter(tf.TiffSequence(directory))})
    client = from_tree(tree)
    arr = client["A"].read_block(block_input)
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
    client = from_config(config)
    # This whole directory of files is one dataset.
    assert client["sequence"].read().shape == (10, 100, 101)
    # This directory has one dataset per file, in the normal fashion.
    client["other_stuff"]["image"].read()
    assert list(client["other_stuff"]["stuff"].read().columns) == ["a", "b", "c"]
    assert list(client["other_file1"].read().columns) == ["a", "b", "c"]
    assert list(client["other_file2"].read().columns) == ["a", "b", "c"]


def test_rgb(tmpdir):
    "Test an RGB TIFF."
    data = numpy.random.randint(0, 255, (11, 17, 3), dtype="uint8")
    path = Path(tmpdir, "temp.tif")
    tf.imwrite(path, data)

    tree = MapAdapter({"A": TiffAdapter(str(path))})
    client = from_tree(tree)
    arr = client["A"].read()
    assert arr.shape == data.shape
