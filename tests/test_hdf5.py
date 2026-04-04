import os
from types import SimpleNamespace
from unittest.mock import patch

import numpy
import pytest

from tiled.adapters import hdf5 as hdf5_adapters
from tiled.adapters.hdf5 import HDF5Adapter, HDF5ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context, record_history
from tiled.server.app import build_app
from tiled.structures.array import ArrayStructure, BuiltinDtype
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management
from tiled.utils import BrokenLink, ensure_uri, path_from_uri, safe_json_dump
from tiled.utils import tree as tree_util


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
        c.create_dataset("d", data=numpy.arange(9, dtype="int64").reshape((3, 3)))
        c.create_dataset("e", data=numpy.arange(12, dtype="int64").reshape((3, 4)))

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


@pytest.fixture(scope="module")
def example_files_with_chunked_arrays(tmp_path_factory):
    h5py = pytest.importorskip("h5py")
    file_paths = []
    for indx in range(3):
        file_path = tmp_path_factory.mktemp("data").joinpath(
            f"example_chunked_{indx}.h5"
        )
        with h5py.File(file_path, "w") as file:
            a = file.create_group("a")
            a.create_dataset(
                "b",
                data=numpy.arange(120 * indx, 120 * (indx + 1), dtype="int64").reshape(
                    (4, 5, 6)
                ),
                chunks=(1, 2, 3),
            )
            a.create_dataset(
                "c",
                data=numpy.arange(10 * indx, 10 * (indx + 1), dtype="int64").reshape(
                    (10, 1)
                ),
                chunks=(2, 1),
            )
            a.create_dataset(
                "d",
                data=numpy.arange(10 * indx, 10 * (indx + 1), dtype="int64").reshape(
                    (10,)
                ),
                chunks=(3,),
            )
        file_paths.append(file_path)

    yield [ensure_uri(file_path) for file_path in file_paths]


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


def test_update_datasource_with_properties(context, example_file):
    # Register a forcefully reshaped dataset and keep the true chunks in properties
    client = from_context(context)

    h5py = pytest.importorskip("h5py")
    with h5py.File(path_from_uri(example_file), "r") as file:
        true_arr = file["a/b/c/e"][:]

    asset = Asset(
        data_uri=example_file,
        is_directory=False,
        parameter="data_uris",
        num=0,
    )
    data_source = DataSource(
        mimetype="application/x-hdf5",
        assets=[asset],
        structure_family=StructureFamily.array,
        structure=ArrayStructure.from_array(true_arr),
        parameters={"dataset": "a/b/c/e"},
        properties={"chunks": None, "other": 42},
        management=Management.external,
    )

    arr = client.new(
        structure_family=StructureFamily.array,
        data_sources=[data_source],
        key="ds_with_properties",
    )

    assert arr.read().shape == (3, 4)
    assert arr.chunks == ((3,), (4,))
    assert arr.data_sources()[0].properties == {"chunks": None, "other": 42}
    numpy.testing.assert_array_equal(arr.read(), true_arr)

    # Update the DataSource forcefully reshaping the data to (12, 1)
    upd_ds = arr.data_sources()[0]
    upd_ds.structure = ArrayStructure(
        shape=(12, 1),
        chunks=((3, 3, 3, 3), (1,)),
        data_type=upd_ds.structure["data_type"],
    )
    upd_ds.properties.update({"chunks": ((3,), (4,))})  # True chunks of the dataset

    context.http_client.put(
        arr.uri.replace("/metadata/", "/data_source/", 1),
        headers={"Content-Type": "application/json"},
        content=safe_json_dump({"data_source": upd_ds}),
    ).raise_for_status()

    arr = client["ds_with_properties"]

    assert arr.read().shape == (12, 1)
    assert arr.chunks == ((3, 3, 3, 3), (1,))
    assert arr.data_sources()[0].properties["chunks"] == [[3], [4]]
    assert arr.data_sources()[0].properties["other"] == 42
    numpy.testing.assert_array_equal(arr.read().ravel(), true_arr.ravel())


def test_adapter_kwargs(example_file):
    # Test that extra kwargs are passed to HDF5ArrayAdapter via HDF5Adapter
    # when initialized from URIs with `dataset` and `slice` parameters

    adapter = HDF5Adapter.from_uris(
        example_file,
        swmr=True,
        libver="latest",
        locking=False,
        dataset="/a/b/c/d",
        slice="(:, 0:2)",
        squeeze=True,
    )
    assert isinstance(adapter, HDF5ArrayAdapter)
    assert adapter.read().shape == (3, 2)


@pytest.mark.parametrize(
    "shape, error",
    [
        ((3, 4), False),
        ((4, 3), False),
        ((2, 6), False),
        ((2, 1, 2, 3), False),
        ((1, 13), True),
    ],
)
def test_adapter_from_catalog(example_file, shape, error):
    # Test that HDF5Adapter can be initialized from catalog and reshaped
    asset = Asset(
        data_uri=example_file,
        is_directory=False,
        parameter="data_uris",
        num=0,
    )
    data_source = DataSource(
        mimetype="application/x-hdf5",
        assets=[asset],
        structure_family=StructureFamily.array,
        structure=ArrayStructure(
            shape=shape,
            chunks=((i,) for i in shape),
            data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("int64")),
        ),
        parameters={"dataset": "a/b/c/e"},
        management=Management.external,
    )

    empty_node = SimpleNamespace(metadata_={}, specs=[])
    adp = HDF5Adapter.from_catalog(data_source, empty_node, **data_source.parameters)

    if error:
        with pytest.raises(ValueError):
            adp.read()
    else:
        assert adp.read().shape == shape


@pytest.mark.parametrize("num_files", [1, 3])
def test_chunked_arrays_from_uris(example_files_with_chunked_arrays, num_files):
    # Test that chunked arrays can be read and reshaped correctly
    fpaths = example_files_with_chunked_arrays[:num_files]
    tree = HDF5Adapter.from_uris(*fpaths)
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

    arr_b = client["a"]["b"]
    assert arr_b.shape == (4 * num_files, 5, 6)
    assert arr_b.chunks == ((1, 1, 1, 1) * num_files, (2, 2, 1), (3, 3))
    assert arr_b.dtype == "int64"
    arr_true_b = numpy.concatenate(
        [
            numpy.arange(120 * indx, 120 * (indx + 1), dtype="int64").reshape((4, 5, 6))
            for indx in range(num_files)
        ],
        axis=0,
    )
    numpy.testing.assert_array_equal(arr_b.read(), arr_true_b)

    arr_c = client["a"]["c"]
    assert arr_c.shape == (10 * num_files, 1)
    assert arr_c.chunks == ((2, 2, 2, 2, 2) * num_files, (1,))
    assert arr_c.dtype == "int64"
    arr_true_c = numpy.concatenate(
        [
            numpy.arange(10 * indx, 10 * (indx + 1), dtype="int64").reshape((10, 1))
            for indx in range(num_files)
        ],
        axis=0,
    )
    numpy.testing.assert_array_equal(arr_c.read(), arr_true_c)

    arr_d = client["a"]["d"]
    assert arr_d.shape == (10 * num_files,)
    assert arr_d.chunks == ((3, 3, 3, 1) * num_files,)
    assert arr_d.dtype == "int64"
    arr_true_d = numpy.concatenate(
        [
            numpy.arange(10 * indx, 10 * (indx + 1), dtype="int64").reshape((10,))
            for indx in range(num_files)
        ],
        axis=0,
    )
    numpy.testing.assert_array_equal(arr_d.read(), arr_true_d)


@pytest.mark.parametrize("num_files", [1, 3])
@pytest.mark.parametrize("reshape", [True, False])
def test_chunked_arrays_from_catalog(
    example_files_with_chunked_arrays, num_files, reshape
):
    # Test that multiple chunked arrays can be read and reshaped correctly when initialized from catalog
    assets = []
    for indx, fpath in enumerate(example_files_with_chunked_arrays[:num_files]):
        assets.append(
            Asset(
                data_uri=fpath,
                is_directory=False,
                parameter="data_uris",
                num=indx,
            )
        )

    shape = (4 * num_files, 5, 6) if not reshape else (num_files, 4, 5, 6)
    chunks = (
        ((1, 1, 1, 1) * num_files, (2, 2, 1), (3, 3))
        if not reshape
        else ((1,) * num_files, (1, 1, 1, 1), (2, 2, 1), (3, 3))
    )

    data_source = DataSource(
        mimetype="application/x-hdf5",
        assets=assets,
        structure_family=StructureFamily.array,
        structure=ArrayStructure(
            shape=shape,
            chunks=chunks,
            data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("int64")),
        ),
        parameters={"dataset": "a/b"},
        management=Management.external,
    )

    empty_node = SimpleNamespace(metadata_={}, specs=[])
    adp = HDF5Adapter.from_catalog(data_source, empty_node, **data_source.parameters)
    structure = adp.structure()
    assert structure.shape == shape
    assert structure.chunks == chunks
    assert structure.data_type.to_numpy_dtype() == numpy.dtype("int64")

    arr_true = numpy.concatenate(
        [
            numpy.arange(120 * indx, 120 * (indx + 1), dtype="int64").reshape((4, 5, 6))
            for indx in range(num_files)
        ],
        axis=0,
    )
    if reshape:
        arr_true = arr_true.reshape((num_files, 4, 5, 6))
    numpy.testing.assert_array_equal(adp.read(), arr_true)


@pytest.mark.parametrize("swmr", [True, False])
def test_files_opened_and_closed(example_files_with_chunked_arrays, swmr):
    "Test that only the necessary files are opened and that they are closed after reading"
    import tiled

    h5py = pytest.importorskip("h5py")

    # Use the example with two files chunked along a single dimension;
    # total chunks across the two files: ((3, 3, 3, 1)*2, )
    file_uris = example_files_with_chunked_arrays[:2]
    file_paths = [path_from_uri(uri) for uri in file_uris]
    with patch(
        "tiled.adapters.hdf5.h5open", wraps=tiled.adapters.hdf5.h5open
    ) as mock_h5open:
        mock_h5open.assert_not_called()  # No files should be opened yet

        # Tree initialized from the entire file, no dataset provided
        tree = HDF5Adapter.from_uris(*file_uris, swmr=swmr)
        mock_h5open.assert_called()
        assert mock_h5open.call_count == 1

        # Adapter initialized directly from the dataset:
        # Each file is read once to get the structure, and the first one
        # is also read once again to get the metadata
        mock_h5open.reset_mock()
        HDF5ArrayAdapter.from_uris(*file_uris, dataset="a/d", swmr=swmr)
        assert mock_h5open.call_count == 3
        files_opened = [call.args[0].name for call in mock_h5open.call_args_list]
        assert files_opened.count(file_paths[0].name) == 2
        assert files_opened.count(file_paths[1].name) == 1
        assert set(files_opened) == set([fp.name for fp in file_paths])

        # Build the app fromn the tree (HDF5Adapter): all datasets in the file are parsed
        mock_h5open.reset_mock()
        with Context.from_app(build_app(tree)) as context:
            client = from_context(context)
        mock_h5open.assert_called()

        # Initialize the array client -- no need to reopen the files
        mock_h5open.reset_mock()
        arr = client["a"]["d"]
        assert arr.structure().shape == (20,)
        assert arr.structure().chunks == ((3, 3, 3, 1) * 2,)
        assert arr.metadata is not None
        mock_h5open.assert_not_called()

        # Read the entire array: files are opened once to get the specs and then again,
        # four times each, to fetch each chunk separately. Additionally, the first file is opened once
        # adain when initialized from catalog to get the metadata
        assert arr.read() is not None
        # 2 for specs, 4*2 for chunks, 1 for metadata
        assert mock_h5open.call_count == 2 + 4 * 2 + 1
        files_opened = [call.args[0].name for call in mock_h5open.call_args_list]
        # First file: 1 for specs, 4 for chunks, 1 for metadata
        assert files_opened.count(file_paths[0].name) == 4 + 1 + 1
        # Second file: 1 for specs, 4 for chunks
        assert files_opened.count(file_paths[1].name) == 4 + 1

        # Read a slice that only touches one file: only the relevant file should be opened
        mock_h5open.reset_mock()
        assert arr[:1] is not None
        assert mock_h5open.call_count == 4  # 2 for specs, 1 for chunks, 1 for metadata
        files_opened = [call.args[0].name for call in mock_h5open.call_args_list]
        assert files_opened.count(file_paths[0].name) == 3
        assert files_opened.count(file_paths[1].name) == 1  # only to get the specs

        # Read everything from the second file
        mock_h5open.reset_mock()
        assert arr[-10:] is not None
        assert mock_h5open.call_count == 7
        files_opened = [call.args[0].name for call in mock_h5open.call_args_list]
        # First file: 1 for specs, 1 for metadata
        assert files_opened.count(file_paths[0].name) == 1 + 1
        # Second file: 4 for chunks, 1 for specs
        assert files_opened.count(file_paths[1].name) == 4 + 1

        # Read a slice that has one value from each of the files
        mock_h5open.reset_mock()
        assert arr[9:11] is not None
        assert mock_h5open.call_count == 2 + 2 + 1
        files_opened = [call.args[0].name for call in mock_h5open.call_args_list]
        # First file: # 1 for specs, 1 for chunk, 1 for metadata
        assert files_opened.count(file_paths[0].name) == 3
        assert files_opened.count(file_paths[1].name) == 2  # 1 for specs, 1 for chunk

    # Try opening the files directly to check that they are closed after reading
    h5py.File(file_paths[0], "r", swmr=swmr).close()
    h5py.File(file_paths[1], "r", swmr=swmr).close()

    h5py.File(file_paths[0], "r", swmr=not swmr).close()
    h5py.File(file_paths[1], "r", swmr=not swmr).close()
