from pathlib import Path

import numpy
import pytest
import tifffile as tf

from ..readers.tiff_sequence import TiffSequenceReader
from ..client import from_tree
from ..trees.in_memory import Tree


@pytest.fixture
def directory(tmpdir):
    data = numpy.random.random((100, 100))
    for i in range(10):
        tf.imwrite(Path(tmpdir, f"temp{i:05}.tif"), data)
    return str(Path(tmpdir, "*.tif"))


@pytest.mark.parametrize(
    "slice_input, correct_shape",
    [
        (None, (10, 100, 100)),
        (0, (100, 100)),
        (slice(0, 10, 2), (5, 100, 100)),
        ((1, slice(0, 10), slice(0, 10)), (10, 10)),
        ((slice(0, 10), slice(0, 10), slice(0, 10)), (10, 10, 10)),
    ],
)
def test_tiff_sequence(directory, slice_input, correct_shape):
    tree = Tree({"A": TiffSequenceReader(tf.TiffSequence(directory))})
    client = from_tree(tree)
    arr = client["A"].read(slice=slice_input)
    assert arr.shape == correct_shape


@pytest.mark.parametrize("block_input, correct_shape", [((0, 0, 0), (1, 100, 100))])
def test_tiff_sequence_block(directory, block_input, correct_shape):
    tree = Tree({"A": TiffSequenceReader(tf.TiffSequence(directory))})
    client = from_tree(tree)
    arr = client["A"].read_block(block_input)
    assert arr.shape == correct_shape
