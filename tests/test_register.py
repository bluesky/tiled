from pathlib import Path

import h5py
import numpy
import pandas
import pytest
import tifffile
import zarr

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.client.register import DEFAULT_MIMETYPES_BY_FILE_EXT, resolve_mimetype
from tiled.server.app import build_app
from tiled.structures.array import ArrayStructure, BuiltinDtype
from tiled.structures.core import Spec
from tiled.structures.table import TableStructure


def test_resolve_mimetype():
    assert (
        resolve_mimetype(Path("test.csv"), DEFAULT_MIMETYPES_BY_FILE_EXT) == "text/csv"
    )


@pytest.fixture
def files_dir(tmp_path_factory):
    """A directory of external files to register."""
    d = tmp_path_factory.mktemp("external_files")

    # A single TIFF (3, 4).
    tifffile.imwrite(
        str(d / "single_scan.tiff"),
        numpy.arange(12, dtype="uint16").reshape(3, 4),
    )

    # A stack of five (3, 4) TIFFs.
    for i in range(5):
        tifffile.imwrite(
            str(d / f"frame_{i:03d}.tiff"),
            numpy.full((3, 4), i, dtype="uint16"),
        )

    # A CSV table (the header row names the columns).
    pandas.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}).to_csv(
        d / "table.csv", index=False
    )

    # A CSV whose columns share one dtype, so it can also be read as an array.
    pandas.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]}).to_csv(
        d / "grid.csv", index=False
    )

    # A semicolon-delimited CSV table (requires sep=";" to parse correctly).
    pandas.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}).to_csv(
        d / "table_semicolon.csv", index=False, sep=";"
    )

    # Two same-schema partitions, as both CSV and Parquet, for multi-file tables.
    part0 = pandas.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    part1 = pandas.DataFrame({"a": [7, 8], "b": [9.0, 10.0]})
    part0.to_csv(d / "part_0.csv", index=False)
    part1.to_csv(d / "part_1.csv", index=False)
    part0.to_parquet(d / "part_0.parquet")
    part1.to_parquet(d / "part_1.parquet")

    # An HDF5 file with a top-level dataset and a nested group.
    with h5py.File(d / "data.h5", "w") as f:
        f["x"] = numpy.arange(10)
        f.create_group("grp")["y"] = numpy.arange(6).reshape(2, 3)

    # Two HDF5 files sharing a dataset path "/data", for concatenation. Each
    # holds a (3, 4) frame filled with its index, so the two concatenate along
    # the first axis into a (6, 4) array.
    for i in range(2):
        with h5py.File(d / f"scan_{i}.h5", "w") as f:
            f["data"] = numpy.full((3, 4), i, dtype="int64")

    # A Zarr store: a directory-backed (10, 20) array.
    z = zarr.open(
        str(d / "data.zarr"), mode="w", shape=(10, 20), chunks=(5, 5), dtype="i4"
    )
    z[:] = numpy.arange(200).reshape(10, 20)

    return d


@pytest.fixture
def client(tmp_path, files_dir):
    catalog = in_memory(
        writable_storage=str(tmp_path / "data"),
        readable_storage=[str(files_dir)],
    )
    with Context.from_app(build_app(catalog)) as context:
        yield from_context(context).include_data_sources()


@pytest.mark.parametrize(
    "filename, expected_family, is_directory, expected",
    [
        (
            "single_scan.tiff",
            "array",
            False,
            numpy.arange(12, dtype="uint16").reshape(3, 4),
        ),
        (
            "table.csv",
            "table",
            False,
            pandas.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}),
        ),
        (
            "data.zarr",
            "array",
            True,
            numpy.arange(200).reshape(10, 20),
        ),
    ],
)
def test_register_single_source(
    client, files_dir, filename, expected_family, is_directory, expected
):
    """Registering one file (or directory) creates a single dataset node.

    Exercises, for each source type: the server-assigned key (not derived from
    the filename), the inferred structure family, the backing asset(s),
    user-supplied metadata/specs, and that the returned client reads back
    exactly what a fresh lookup does.
    """
    node = client.register(
        str(files_dir / filename),
        metadata={"beamline": "X"},
        specs=[Spec("my_spec")],
    )

    # The server assigns a random key; it is not derived from the filename.
    assert Path(filename).stem not in client
    assert node.item["id"] in client

    # The inferred structure family and the backing asset(s).
    assert node.structure_family == expected_family
    (data_source,) = node.data_sources()
    assert data_source.assets
    assert all(asset.is_directory == is_directory for asset in data_source.assets)

    # User metadata/specs are applied on top of anything read from the file.
    assert node.metadata["beamline"] == "X"
    assert Spec("my_spec") in node.specs

    # The data round-trips, and the returned client agrees with a fresh lookup.
    fresh = client[node.item["id"]]
    if isinstance(expected, pandas.DataFrame):
        pandas.testing.assert_frame_equal(fresh.read().reset_index(drop=True), expected)
        pandas.testing.assert_frame_equal(node.read(), fresh.read())
    else:
        numpy.testing.assert_array_equal(fresh.read(), expected)
        numpy.testing.assert_array_equal(node.read(), fresh.read())


def test_register_keys(client, files_dir):
    """Server-assigned keys are unique per call; explicit keys are honored.

    Registering the same file twice (without a key) yields two distinct nodes
    that nonetheless point at the very same underlying asset.
    """
    path = str(files_dir / "single_scan.tiff")
    node1 = client.register(path)
    node2 = client.register(path)
    assert node1.item["id"] != node2.item["id"]
    assert len(client) == 2

    # Both nodes are backed by the same external file (same asset URI).
    (ds1,) = node1.data_sources()
    (ds2,) = node2.data_sources()
    assert [a.data_uri for a in ds1.assets] == [a.data_uri for a in ds2.assets]

    # An explicit key is used verbatim.
    node3 = client.register(path, key="explicit")
    assert node3.item["id"] == "explicit"
    assert "explicit" in client


def test_register_stack_of_files(client, files_dir):
    "Multiple files register as a single stacked array (not multiple nodes)."
    paths = [str(files_dir / f"frame_{i:03d}.tiff") for i in range(5)]
    node = client.register(*paths, key="my_stack")
    arr = node.read()
    assert arr.shape == (5, 3, 4)
    for i in range(5):
        assert (arr[i] == i).all()
    # It is one node, backed by five ordered assets.
    (data_source,) = node.data_sources()
    assert len(data_source.assets) == 5


@pytest.mark.parametrize(
    "filenames, expected_a",
    [
        (["table.csv"], [1, 2, 3]),
        (["part_0.csv", "part_1.csv"], [1, 2, 3, 7, 8]),
        (["part_0.parquet"], [1, 2, 3]),
        (["part_0.parquet", "part_1.parquet"], [1, 2, 3, 7, 8]),
    ],
)
def test_register_partitioned_table(client, files_dir, filenames, expected_a):
    """CSV and Parquet register as a single table, from one or several files.

    Several same-schema files are combined into one table node (one asset per
    file), with the mimetype inferred from the extension.
    """
    paths = [str(files_dir / name) for name in filenames]
    node = client.register(*paths, key="tbl")
    assert node.structure_family == "table"
    df = node.read()
    assert list(df.columns) == ["a", "b"]
    assert sorted(df["a"].tolist()) == sorted(expected_a)
    # One node, backed by one asset per file.
    (data_source,) = node.data_sources()
    assert len(data_source.assets) == len(filenames)


def test_register_hdf5(client, files_dir):
    """An HDF5 file registers as one container node exposing its datasets.

    This is still a single registered node backed by a single asset (the file);
    its datasets are served as sub-nodes, without walking any directory.
    """
    node = client.register(str(files_dir / "data.h5"), key="h5")
    assert node.structure_family == "container"
    assert set(node) >= {"x", "grp"}
    numpy.testing.assert_array_equal(node["x"].read(), numpy.arange(10))
    numpy.testing.assert_array_equal(
        node["grp"]["y"].read(), numpy.arange(6).reshape(2, 3)
    )
    # It is backed by the single HDF5 file.
    (data_source,) = client["h5"].data_sources()
    assert len(data_source.assets) == 1


@pytest.mark.parametrize(
    "dataset, expected",
    [
        ("x", numpy.arange(10)),
        ("grp/y", numpy.arange(6).reshape(2, 3)),
    ],
)
def test_register_hdf5_dataset(client, files_dir, dataset, expected):
    """A single dataset within an HDF5 file registers as an array node.

    Passing `parameters={"dataset": ...}` selects a specific dataset (which may
    be nested in a group) instead of registering the whole file as a container.
    """
    node = client.register(
        str(files_dir / "data.h5"),
        key="ds",
        parameters={"dataset": dataset},
    )
    # It is an array node (not a container), backed by the single file.
    assert node.structure_family == "array"
    numpy.testing.assert_array_equal(node.read(), expected)
    (data_source,) = node.data_sources()
    assert len(data_source.assets) == 1
    # The dataset selection is persisted so the server re-opens the same path.
    assert data_source.parameters.get("dataset") == dataset


def test_register_hdf5_dataset_concatenated(client, files_dir):
    """Several HDF5 files sharing a dataset path register as one concatenated array.

    Each file holds a (3, 4) frame; the two are concatenated along the first
    axis into a single (6, 4) array node backed by one asset per file.
    """
    paths = [str(files_dir / f"scan_{i}.h5") for i in range(2)]
    node = client.register(*paths, key="concat", parameters={"dataset": "data"})
    assert node.structure_family == "array"
    arr = node.read()
    assert arr.shape == (6, 4)
    numpy.testing.assert_array_equal(arr[:3], numpy.zeros((3, 4), dtype="int64"))
    numpy.testing.assert_array_equal(arr[3:], numpy.ones((3, 4), dtype="int64"))
    # One node, backed by one asset per file, in order.
    (data_source,) = node.data_sources()
    assert len(data_source.assets) == 2


def test_register_csv_as_table_or_array(client, files_dir):
    """The same CSV can be registered as a table or, via mimetype, as an array.

    As a table (the default) the header row names the columns. As an array
    (via the ``header=absent`` mimetype) we keep only the values, skipping the
    header row with ``parameters={"skiprows": 1}``.
    """
    path = str(files_dir / "grid.csv")

    # Default: a table whose column names come from the CSV header.
    table = client.register(path, key="as_table")
    assert table.structure_family == "table"
    df = table.read()
    assert list(df.columns) == ["a", "b"]
    numpy.testing.assert_array_equal(df["a"].values, [1.0, 2.0, 3.0])

    # As an array: skip the header row so only the values are kept.
    array = client.register(
        path,
        key="as_array",
        mimetype="text/csv;header=absent",
        parameters={"skiprows": 1},
    )
    assert array.structure_family == "array"
    out = array.read()
    assert out.shape == (3, 2)  # 3 data rows, 2 columns; header row dropped
    numpy.testing.assert_array_equal(out, [[1.0, 4.0], [2.0, 5.0], [3.0, 6.0]])


def test_register_with_parameters(client, files_dir):
    "Adapter open parameters are used client-side and persisted for the server."
    node = client.register(
        str(files_dir / "table_semicolon.csv"),
        key="semicolon",
        parameters={"sep": ";"},
    )
    # The parameter let the client infer the correct two-column structure.
    df = node.read()
    assert list(df.columns) == ["a", "b"]
    numpy.testing.assert_array_equal(df["a"].values, [1, 2, 3])
    numpy.testing.assert_array_equal(df["b"].values, [4.0, 5.0, 6.0])
    # It is persisted on the DataSource so the server re-opens the file the same way.
    (data_source,) = node.data_sources()
    assert data_source.parameters.get("sep") == ";"


def test_register_structure_override_reshapes(client, files_dir):
    """A compatible structure reshapes the data on read.

    The original on-disk chunking is recorded in the DataSource properties so
    the server can still read the file(s) correctly.
    """
    path = str(files_dir / "single_scan.tiff")
    base = client.register(path, key="base").structure()
    reshaped = ArrayStructure(
        data_type=base.data_type,
        chunks=((2,), (6,)),
        shape=(2, 6),
        dims=None,
    )
    node = client.register(path, key="reshaped", structure=reshaped)
    out = node.read()
    assert out.shape == (2, 6)
    numpy.testing.assert_array_equal(out, numpy.arange(12).reshape(2, 6))

    # The original chunks are recorded (as nested lists after a JSON round-trip).
    (data_source,) = node.data_sources()
    stored = data_source.properties["chunks"]
    assert [tuple(dim) for dim in stored] == [tuple(dim) for dim in base.chunks]


def _bad_shape(base):
    # 14 elements instead of 12: a different element count.
    return ArrayStructure(
        data_type=base.data_type, chunks=((2,), (7,)), shape=(2, 7), dims=None
    )


def _bad_dtype(base):
    return ArrayStructure(
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("float64")),
        chunks=base.chunks,
        shape=base.shape,
        dims=None,
    )


def _wrong_column_count(base):
    return TableStructure.from_schema(base.arrow_schema_decoded.remove(1))


@pytest.mark.parametrize(
    "filename, make_bad_structure",
    [
        ("single_scan.tiff", _bad_shape),
        ("single_scan.tiff", _bad_dtype),
        ("table.csv", _wrong_column_count),
    ],
)
def test_register_incompatible_structure_rejected(
    client, files_dir, filename, make_bad_structure
):
    "An override that cannot serve the file(s) is rejected; nothing is registered."
    path = str(files_dir / filename)
    base = client.register(path, key="base").structure()
    with pytest.raises(ValueError, match="not compatible"):
        client.register(path, key="rejected", structure=make_bad_structure(base))
    assert "rejected" not in client


def test_register_errors(client, tmp_path):
    "Registering with no URIs, or an unknown file type, raises."
    # No URIs provided.
    with pytest.raises(ValueError):
        client.register()
    # A file whose extension maps to no known mimetype.
    mystery = tmp_path / "mystery.unknownext"
    mystery.write_bytes(b"\x00\x01")
    with pytest.raises(ValueError):
        client.register(str(mystery))
