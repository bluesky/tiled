import numpy
import pytest

from ..adapters import hdf5 as hdf5_adapters
from ..adapters.hdf5 import HDF5Adapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context, record_history
from ..server.app import build_app
from ..utils import ensure_uri
from ..utils import tree as tree_util


@pytest.fixture(scope="module")
def example_file(tmp_path_factory):
    h5py = pytest.importorskip("h5py")
    file_path = tmp_path_factory.mktemp("data").joinpath("example.h5")
    with h5py.File(file_path, "w") as file:
        a = file.create_group("a")
        b = a.create_group("b")
        c = b.create_group("c")
        c.create_dataset("d", data=numpy.ones((3, 3)))
    return ensure_uri(file_path)


@pytest.fixture(scope="module")
def example_file_with_vlen_str_in_dataset(tmp_path_factory):
    h5py = pytest.importorskip("h5py")
    file_path = tmp_path_factory.mktemp("data").joinpath("example_with_vlen_str.h5")
    with h5py.File(file_path, "w") as file:
        a = file.create_group("a")
        b = a.create_group("b")
        c = b.create_group("c")
        # Need to do this to make a vlen str dataset
        dt = h5py.string_dtype(encoding="utf-8")
        dset = c.create_dataset("d", (100,), dtype=dt)
        assert dset.dtype == "object"
        dset[0] = b"test"
    return ensure_uri(file_path)


def test_from_file(example_file, buffer):
    """Serve a single HDF5 file at top level."""
    h5py = pytest.importorskip("h5py")
    tree = HDF5Adapter.from_uris(example_file)
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        arr = client["a"]["b"]["c"]["d"].read()
        assert isinstance(arr, numpy.ndarray)
        client.export(buffer, format="application/x-hdf5")
        file = h5py.File(buffer, "r")
        file["a"]["b"]["c"]["d"]


def test_from_file_with_vlen_str_dataset(example_file_with_vlen_str_in_dataset, buffer):
    """Serve a single HDF5 file at top level."""
    h5py = pytest.importorskip("h5py")
    tree = HDF5Adapter.from_uris(example_file_with_vlen_str_in_dataset)
    with pytest.warns(UserWarning):
        with Context.from_app(build_app(tree)) as context:
            client = from_context(context)
    arr = client["a"]["b"]["c"]["d"].read()
    assert isinstance(arr, numpy.ndarray)
    with pytest.warns(UserWarning):
        client.export(buffer, format="application/x-hdf5")
    file = h5py.File(buffer, "r")
    file["a"]["b"]["c"]["d"]


def test_from_group(example_file, buffer):
    """Serve a Group within an HDF5 file."""
    h5py = pytest.importorskip("h5py")
    tree = HDF5Adapter.from_uris(example_file, dataset="a/b")
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    arr = client["c"]["d"].read()
    assert isinstance(arr, numpy.ndarray)
    client.export(buffer, format="application/x-hdf5")
    file = h5py.File(buffer, "r")
    file["c"]["d"]


def test_from_multiple(example_file, buffer):
    """Serve two files within a mapping."""
    h5py = pytest.importorskip("h5py")
    tree = MapAdapter(
        {
            "A": HDF5Adapter.from_uris(example_file),
            "B": HDF5Adapter.from_uris(example_file),
        }
    )
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    arr_A = client["A"]["a"]["b"]["c"]["d"].read()
    assert isinstance(arr_A, numpy.ndarray)
    arr_B = client["B"]["a"]["b"]["c"]["d"].read()
    assert isinstance(arr_B, numpy.ndarray)
    client.export(buffer, format="application/x-hdf5")
    file = h5py.File(buffer, "r")
    file["A"]["a"]["b"]["c"]["d"]
    file["B"]["a"]["b"]["c"]["d"]


def test_inlined_contents(example_file):
    """Test that the recursive structure and metadata are inlined into one request."""
    tree = HDF5Adapter.from_uris(example_file)
    assert hdf5_adapters.INLINED_DEPTH > 1
    original = hdf5_adapters.INLINED_DEPTH
    try:
        with Context.from_app(build_app(tree)) as context:
            with record_history() as hN:
                client = from_context(context)
                tree_util(client)
            hdf5_adapters.INLINED_DEPTH = 1
            with record_history() as h1:
                client = from_context(context)
                tree_util(client)
            hdf5_adapters.INLINED_DEPTH = 0
            with record_history() as h0:
                client = from_context(context)
                tree_util(client)
            assert len(h0.requests) > len(h1.requests) > len(hN.requests)
    finally:
        hdf5_adapters.INLINED_DEPTH = original
