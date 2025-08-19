import os

import numpy
import pytest

from ..adapters import hdf5 as hdf5_adapters
from ..adapters.hdf5 import HDF5Adapter
from ..adapters.mapping import MapAdapter
from ..catalog import in_memory
from ..client import Context, from_context, record_history
from ..server.app import build_app
from ..structures.core import StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..utils import BrokenLink, ensure_uri, path_from_uri
from ..utils import tree as tree_util


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    return in_memory(writable_storage=tmp_path_factory.getbasetemp())


@pytest.fixture(scope="module")
def context(tree):
    with Context.from_app(build_app(tree)) as context:
        yield context


@pytest.fixture(scope="module")
def example_file(tmp_path_factory):
    h5py = pytest.importorskip("h5py")
    file_path = tmp_path_factory.mktemp("data").joinpath("example.h5")
    with h5py.File(file_path, "w") as file:
        a = file.create_group("a")
        b = a.create_group("b")
        c = b.create_group("c")
        c.create_dataset("d", data=numpy.ones((3, 3)))

    yield ensure_uri(file_path)


@pytest.fixture(scope="module")
def example_file_with_empty_data(tmp_path_factory):
    h5py = pytest.importorskip("h5py")
    file_path = tmp_path_factory.mktemp("data").joinpath("example_with_empty.h5")
    with h5py.File(file_path, "w") as file:
        a = file.create_group("a")
        b = a.create_group("b")
        c = b.create_group("c")
        c.create_dataset("d", data=numpy.empty(shape=0))
        c.create_dataset("e", data=numpy.empty(shape=(5, 7)))
        c.create_dataset("f", data=numpy.empty(shape=1, dtype="S0"))
        c.create_dataset("g", data=[])
        c.create_dataset("h", data="")
        c.create_dataset("i", data=numpy.empty(shape=()))

    yield ensure_uri(file_path)


@pytest.fixture(scope="module")
def example_file_with_scalars(tmp_path_factory):
    h5py = pytest.importorskip("h5py")
    file_path = tmp_path_factory.mktemp("data").joinpath("example_with_scalars.h5")
    with h5py.File(file_path, "w") as file:
        a = file.create_group("a")
        b = a.create_group("b")
        c = b.create_group("c")
        c.create_dataset("int", data=42)
        c.create_dataset("float", data=3.14)
        c.create_dataset("str", data="hello")
        c.create_dataset("bytes", data=b"hello")
        c.create_dataset("bool", data=True)

    yield ensure_uri(file_path)


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

    yield ensure_uri(file_path)


@pytest.fixture(scope="function")
def example_file_with_links(tmp_path_factory):
    h5py = pytest.importorskip("h5py")
    file_path = tmp_path_factory.mktemp("data").joinpath("example.h5")
    with h5py.File(file_path.with_name("linked.h5"), "w") as file:
        z = file.create_group("z")
        y = z.create_group("y")
        y.create_dataset("x", data=2 * numpy.ones((5, 5)))
    with h5py.File(file_path, "w") as file:
        a = file.create_group("a")
        b = a.create_group("b")
        c = b.create_group("c")
        c.create_dataset("d", data=numpy.ones((3, 3)))
        b["hard_link"] = c["d"]
        b["soft_link"] = h5py.SoftLink("/a/b/c/d")
        b["extr_link"] = h5py.ExternalLink("linked.h5", "/z/y")

    yield ensure_uri(file_path)


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
        assert file["a"]["b"]["c"]["d"] is not None


@pytest.mark.filterwarnings("ignore: The dataset")
@pytest.mark.parametrize("key", ["d", "e", "f", "g", "h", "i"])
def test_from_file_with_empty_data(example_file_with_empty_data, buffer, key):
    """Serve a single HDF5 file at top level."""
    h5py = pytest.importorskip("h5py")
    tree = HDF5Adapter.from_uris(example_file_with_empty_data)
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        arr = client["a"]["b"]["c"][key].read()
        assert isinstance(arr, numpy.ndarray)
        client.export(buffer, format="application/x-hdf5")
        file = h5py.File(buffer, "r")
        assert file["a"]["b"]["c"][key] is not None


@pytest.mark.filterwarnings("ignore: The dataset")
@pytest.mark.parametrize("key", ["bool", "int", "float", "str", "bytes"])
@pytest.mark.parametrize("num", [1, 5])
def test_from_file_with_scalars(example_file_with_scalars, buffer, key: str, num: int):
    """Serve HDF5 file(s) containing scalars."""
    if key in {"str", "bytes"}:
        pytest.xfail("HDF5Adapter treats object dtypes differently.")
    h5py = pytest.importorskip("h5py")
    tree = HDF5Adapter.from_uris(*[example_file_with_scalars] * num)
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        arr = client["a"]["b"]["c"][key].read()
        assert isinstance(arr, numpy.ndarray)
        assert arr.shape == (num,)
        client.export(buffer, format="application/x-hdf5")
        file = h5py.File(buffer, "r")
        assert file["a"]["b"]["c"][key] is not None


@pytest.mark.filterwarnings("ignore: The dataset")
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
    assert file["a"]["b"]["c"]["d"] is not None


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
    assert file["c"]["d"] is not None


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
    assert file["A"]["a"]["b"]["c"]["d"] is not None
    assert file["B"]["a"]["b"]["c"]["d"] is not None


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


def test_file_with_links(example_file_with_links, buffer):
    """Serve an HDF5 file with internal and external links."""

    h5py = pytest.importorskip("h5py")
    tree = HDF5Adapter.from_uris(example_file_with_links)
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

    # Read the original array
    arr = client["a/b/c/d"].read()
    arr = client["a"]["b/c/d"].read()
    arr = client["a"]["b"]["c/d"].read()
    arr = client["a"]["b"]["c"]["d"].read()
    arr = client["a/b/c"]["d"].read()
    assert isinstance(arr, numpy.ndarray)
    assert numpy.allclose(arr, numpy.ones((3, 3)))

    # Read the hard link
    arr = client["a"]["b"]["hard_link"].read()
    assert isinstance(arr, numpy.ndarray)
    assert numpy.allclose(arr, numpy.ones((3, 3)))

    # Read the soft link
    arr = client["a"]["b"]["soft_link"].read()
    assert isinstance(arr, numpy.ndarray)
    assert numpy.allclose(arr, numpy.ones((3, 3)))

    # Read the external link
    arr = client["a"]["b"]["extr_link/x"].read()
    assert isinstance(arr, numpy.ndarray)
    assert numpy.allclose(arr, 2 * numpy.ones((5, 5)))

    # Export the tree into another file/buffer
    client.export(buffer, format="application/x-hdf5")
    file = h5py.File(buffer, "r")
    assert numpy.allclose(numpy.array(file["a/b/c/d"]), numpy.ones((3, 3)))
    assert numpy.allclose(numpy.array(file["a/b/hard_link"]), numpy.ones((3, 3)))
    assert numpy.allclose(numpy.array(file["a/b/soft_link"]), numpy.ones((3, 3)))
    assert numpy.allclose(numpy.array(file["a/b/extr_link/x"]), 2 * numpy.ones((5, 5)))


def test_file_with_broken_links(example_file_with_links):
    """Raise an error when accessing non-existing keys."""

    h5py = pytest.importorskip("h5py")
    main_file_path = path_from_uri(example_file_with_links)
    child_file_path = main_file_path.with_name("linked.h5")

    # Case 1. Broken soft link
    # KeyError: 'Unable to synchronously open object (component not found)'
    with h5py.File(main_file_path, "r+") as file:
        file["a/b/c"].pop("d")

    with pytest.raises(BrokenLink):
        tree = HDF5Adapter.from_uris(example_file_with_links, dataset="a/b/soft_link")

    with pytest.raises(KeyError):
        tree = HDF5Adapter.from_uris(example_file_with_links, dataset="a/b/c/d")

    tree = HDF5Adapter.from_uris(example_file_with_links, dataset="a")
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert len(client["b"]) == 4  # All keys are there
    with pytest.raises(KeyError):
        client["b/soft_link"]

    # Case 2. Broken children of an external link
    # KeyError when accessing 'x'; 'y' is still accessible (but empty)
    with h5py.File(child_file_path, "r+") as file:
        file["z/y"].pop("x")

    with pytest.raises(KeyError):
        tree = HDF5Adapter.from_uris(example_file_with_links, dataset="a/b/extr_link/x")

    tree = HDF5Adapter.from_uris(example_file_with_links, dataset="a/b/extr_link")
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert len(client) == 0
    with pytest.raises(KeyError):
        client["x"]

    # Case 3. Broken external link -- the referenced object is missing
    # KeyError: "Unable to synchronously open object (object 'y' doesn't exist)"
    with h5py.File(child_file_path, "r+") as file:
        file["z"].pop("y")

    with pytest.raises(KeyError):
        tree = HDF5Adapter.from_uris(example_file_with_links, dataset="a/b/extr_link")

    # A client can still be instantiated
    tree = HDF5Adapter.from_uris(example_file_with_links)
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

    # Case 4. Broken external link -- the file is missing
    # KeyError: "Unable to synchronously open object (unable to open external file, external link file name = 'linked.h5')"  # noqa
    os.remove(child_file_path)

    with pytest.raises(BrokenLink):
        tree = HDF5Adapter.from_uris(example_file_with_links, dataset="a/b/extr_link")

    tree = HDF5Adapter.from_uris(example_file_with_links)
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    with pytest.raises(KeyError):
        client["a/b/extr_link"]


def test_register_broken_hdf5_file(context, example_file_with_links):
    """Test that a broken HDF5 file can be registered and accessed."""

    client = from_context(context)

    h5py = pytest.importorskip("h5py")
    main_file_path = path_from_uri(example_file_with_links)
    child_file_path = main_file_path.with_name("linked.h5")

    # Brake the soft link
    with h5py.File(main_file_path, "r+") as file:
        file["a/b/c"].pop("d")

    # Brake the external link
    os.remove(child_file_path)

    asset = Asset(
        data_uri=f"file://localhost/{main_file_path}",
        is_directory=False,
        parameter="data_uris",
        num=0,
    )

    data_source_from_root = DataSource(
        mimetype="application/x-hdf5",
        assets=[asset],
        structure_family=StructureFamily.container,
        structure=None,
        management=Management.external,
    )

    data_source_from_node = DataSource(
        mimetype="application/x-hdf5",
        assets=[asset],
        structure_family=StructureFamily.container,
        structure=None,
        parameters={"dataset": "a/b"},
        management=Management.external,
    )

    data_source_from_soft = DataSource(
        mimetype="application/x-hdf5",
        assets=[asset],
        structure_family=StructureFamily.container,
        structure=None,
        parameters={"dataset": "a/b/soft_link"},
        management=Management.external,
    )

    data_source_from_extr = DataSource(
        mimetype="application/x-hdf5",
        assets=[asset],
        structure_family=StructureFamily.container,
        structure=None,
        parameters={"dataset": "a/b/extr_link"},
        management=Management.external,
    )

    client.new(
        structure_family=StructureFamily.container,
        data_sources=[data_source_from_root],
        key="ds_from_root",
    )

    client.new(
        structure_family=StructureFamily.container,
        data_sources=[data_source_from_node],
        key="ds_from_node",
    )

    client.new(
        structure_family=StructureFamily.container,
        data_sources=[data_source_from_soft],
        key="ds_from_soft",
    )

    client.new(
        structure_family=StructureFamily.container,
        data_sources=[data_source_from_extr],
        key="ds_from_extr",
    )

    assert len(client) == 4  # All registered
    assert set(client.keys()) == {
        "ds_from_root",
        "ds_from_node",
        "ds_from_soft",
        "ds_from_extr",
    }
    assert list(client["ds_from_root"].keys()) is not None
    assert list(client["ds_from_node"].keys()) is not None

    # Datasets referenced from the root of hdf5 file
    with pytest.raises(KeyError):
        client["ds_from_root"]["a/b/soft_link"]

    with pytest.raises(KeyError):
        client["ds_from_root"]["a/b/extr_link"]

    # Datasets referenced from an internal node of hdf5 file
    with pytest.raises(KeyError):
        client["ds_from_node"]["soft_link"]

    with pytest.raises(KeyError):
        client["ds_from_node"]["extr_link"]

    # Datasets referenced from links directly
    with pytest.raises(KeyError):
        list(client["ds_from_soft"].keys())

    with pytest.raises(KeyError):
        list(client["ds_from_extr"].keys())
