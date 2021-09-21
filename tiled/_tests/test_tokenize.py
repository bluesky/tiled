from pathlib import Path

import h5py
import numpy

from ..server.etag import tokenize


def test_deterministic_h5py_dataset(tmpdir):
    file = h5py.File(Path(tmpdir, "test.h5"), "w")
    group = file.create_group("stuff")
    dataset = group.create_dataset("data", data=numpy.ones((3, 3)))
    assert tokenize(dataset) == tokenize(dataset)
