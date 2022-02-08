import io

import numpy
import pytest

from ..adapters.hdf5 import HDF5Adapter
from ..adapters.mapping import MapAdapter
from ..client import from_tree


@pytest.fixture
def example_file():
    h5py = pytest.importorskip("h5py")
    file = h5py.File(io.BytesIO(), "w")
    a = file.create_group("a")
    b = a.create_group("b")
    c = b.create_group("c")
    c.create_dataset("d", data=numpy.ones((3, 3)))
    return file


@pytest.fixture
def example_file_with_vlen_str_in_dataset():
    h5py = pytest.importorskip("h5py")
    file = h5py.File(io.BytesIO(), "w")
    a = file.create_group("a")
    b = a.create_group("b")
    c = b.create_group("c")
    # Need to do this to make a vlen str dataset
    dt = h5py.string_dtype(encoding="utf-8")
    dset = c.create_dataset("d", (100,), dtype=dt)
    # print(dset.dtype)
    dset[0] = b"test"
    return file


def test_from_file(example_file):
    """Serve a single HDF5 file at top level."""
    h5py = pytest.importorskip("h5py")
    tree = HDF5Adapter(example_file)
    client = from_tree(tree)
    arr = client["a"]["b"]["c"]["d"].read()
    assert isinstance(arr, numpy.ndarray)
    buffer = io.BytesIO()
    client.export(buffer, format="application/x-hdf5")
    file = h5py.File(buffer, "r")
    file["a"]["b"]["c"]["d"]


def test_from_file_with_vlen_str_dataset(example_file_with_vlen_str_in_dataset):
    """Serve a single HDF5 file at top level."""
    h5py = pytest.importorskip("h5py")
    tree = HDF5Adapter(example_file_with_vlen_str_in_dataset)
    client = from_tree(tree)
    with pytest.warns(UserWarning):
        arr = client["a"]["b"]["c"]["d"].read()
    assert isinstance(arr, numpy.ndarray)
    buffer = io.BytesIO()
    with pytest.warns(UserWarning):
        client.export(buffer, format="application/x-hdf5")
    file = h5py.File(buffer, "r")
    file["a"]["b"]["c"]["d"]


def test_from_group(example_file):
    """Serve a Group within an HDF5 file."""
    h5py = pytest.importorskip("h5py")
    tree = HDF5Adapter(example_file["a"]["b"])
    client = from_tree(tree)
    arr = client["c"]["d"].read()
    assert isinstance(arr, numpy.ndarray)
    buffer = io.BytesIO()
    client.export(buffer, format="application/x-hdf5")
    file = h5py.File(buffer, "r")
    file["c"]["d"]


def test_from_multiple(example_file):
    """Serve two files within a mapping."""
    h5py = pytest.importorskip("h5py")
    tree = MapAdapter(
        {
            "A": HDF5Adapter(example_file),
            "B": HDF5Adapter(example_file),
        }
    )
    client = from_tree(tree)
    arr_A = client["A"]["a"]["b"]["c"]["d"].read()
    assert isinstance(arr_A, numpy.ndarray)
    arr_B = client["B"]["a"]["b"]["c"]["d"].read()
    assert isinstance(arr_B, numpy.ndarray)
    buffer = io.BytesIO()
    client.export(buffer, format="application/x-hdf5")
    file = h5py.File(buffer, "r")
    file["A"]["a"]["b"]["c"]["d"]
    file["B"]["a"]["b"]["c"]["d"]
