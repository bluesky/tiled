import awkward
import numpy
import pandas
import pyarrow
import pytest
import ragged
import sparse
import tifffile as tf
import xarray
from starlette.status import HTTP_409_CONFLICT

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.client.composite import CompositeClient
from tiled.client.container import Container
from tiled.client.utils import ClientError
from tiled.server.app import build_app
from tiled.structures.array import ArrayStructure, BuiltinDtype
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management
from tiled.structures.table import TableStructure
from tiled.utils import ensure_uri

from .utils import fail_with_status_code

rng = numpy.random.default_rng(12345)

df1 = pandas.DataFrame({"A": ["one", "two", "three"], "B": [1, 2, 3]})
df2 = pandas.DataFrame(
    {
        "C": ["red", "green", "blue", "white"],
        "D": [10.0, 20.0, 30.0, 40.0],
        "E": [0, 0, 0, 0],
    }
)
df3 = pandas.DataFrame(
    {
        "col1": ["one", "two", "three", "four", "five"],
        "col2": [1.0, 2.0, 3.0, 4.0, 5.0],
    }
)
arr1 = rng.random(size=(3, 5), dtype="float64")
arr2 = rng.integers(0, 255, size=(5, 7, 3), dtype="uint8")
tab1 = pyarrow.Table.from_pydict({"H": [1, 2, 3], "I": [4, 5, 6]})
tab2 = pyarrow.Table.from_pydict({"J": [1, 2], "K": [3, 4], "L": [5, 6]})
img_data = rng.integers(0, 255, size=(5, 13, 17, 3), dtype="uint8")

# An awkward array
awk_arr = awkward.Array(
    [
        [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
        [],
        [{"x": 3.3, "y": [1, 2, 3]}],
    ]
)
awk_packed = awkward.to_packed(awk_arr)
awk_form, awk_length, awk_container = awkward.to_buffers(awk_packed)

# A sparse array
arr = rng.random(size=(10, 20, 30), dtype="float64")
sps_arr = sparse.COO(numpy.where(arr > 0.95, arr, 0))

# A ragged (jagged) array — cannot be converted to numpy
rag_arr = ragged.array([[1.1, 2.2, 3.3], [4.4], [5.5, 6.6]])

# A "regular" ragged array — every row the same length, so numpy-convertible
rag_regular = ragged.array(rng.random(size=(4, 3), dtype="float64").tolist())

md = {"md_key1": "md_val1", "md_key2": 2}


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    tempdir = tmp_path_factory.getbasetemp()
    return in_memory(
        writable_storage=[
            ensure_uri(tempdir / "data"),
            f"duckdb:///{tempdir / 'data.duckdb'}",
        ],
        readable_storage=[ensure_uri(tempdir)],
    )


@pytest.fixture(scope="module")
def context(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        x = client.create_container(key="x", metadata=md, specs=["composite"])
        x.write_array(arr1, key="arr1", metadata={"md_key": "md_for_arr1"})
        x.write_array(arr2, key="arr2", metadata={"md_key": "md_for_arr2"})
        x.write_table(
            df1,
            key="df1",
            metadata={
                "md_key": "md_for_df1",
                "A": {"md_key": "md_for_A"},
                "B": {"md_key": "md_for_B"},
            },
        )
        x.write_table(
            df2,
            key="df2",
            metadata={
                "md_key": "md_for_df2",
                "C": {"md_key": "md_for_C"},
                "D": {"md_key": "md_for_D"},
                "E": {"md_key": "md_for_E"},
            },
        )
        x.write_awkward(awk_arr, key="awk", metadata={"md_key": "md_for_awk"})
        x.write_sparse(
            coords=sps_arr.coords,
            data=sps_arr.data,
            shape=sps_arr.shape,
            key="sps",
            metadata={"md_key": "md_for_sps"},
        )
        x.write_ragged(rag_arr, key="rag", metadata={"md_key": "md_for_rag"})

        yield context


@pytest.fixture(scope="function")
def context_for_reading(context):
    client = from_context(context)

    # Awkward arrays are not supported when building xarray, in general
    client["x"].delete_contents("awk", external_only=False)
    # Jagged ragged arrays are not numpy-convertible either; replace with a
    # regular (rectangular) ragged array so the xarray export works.
    client["x"].delete_contents("rag", external_only=False)
    client["x"].write_ragged(rag_regular, key="rag")
    # Add an image array and a table with 5 rows
    client["x"].write_array(img_data, key="img")
    client["x"].write_table(df3, key="df3")

    yield context

    # Restore the original context
    client["x"].write_awkward(awk_arr, key="awk", metadata={"md_key": "md_for_awk"})
    client["x"].delete_contents("img", external_only=False)
    client["x"].delete_contents("df3", external_only=False)
    client["x"].delete_contents("rag", external_only=False)
    client["x"].write_ragged(rag_arr, key="rag", metadata={"md_key": "md_for_rag"})


@pytest.fixture(scope="function")
def client_for_writing(context):
    # Client for tests that would create a composite node "z"; remove it afterwards
    client = from_context(context)

    yield client

    client.delete_contents("z", external_only=False, recursive=True)


@pytest.fixture
def tiff_sequence(tmp_path_factory):
    tempdir = tmp_path_factory.mktemp("sequence-")
    filepaths = []
    for i in range(img_data.shape[0]):
        fpath = tempdir / f"temp{i:05}.tif"
        tf.imwrite(fpath, img_data[i, ...])
        filepaths.append(fpath)

    yield filepaths


@pytest.fixture
def csv_file(tmp_path_factory):
    tempdir = tmp_path_factory.mktemp("csv-")
    fpath = tempdir / "test.csv"
    df3.to_csv(fpath, index=False)

    yield fpath


@pytest.fixture
def tiff_data_source(tiff_sequence):
    tiff_assets = [
        Asset(
            data_uri=ensure_uri(fpath),
            is_directory=False,
            parameter="data_uris",
            num=i + 1,
        )
        for i, fpath in enumerate(tiff_sequence)
    ]
    tiff_structure_0 = ArrayStructure(
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("uint8")),
        shape=(5, 13, 17, 3),
        chunks=((1, 1, 1, 1, 1), (13,), (17,), (3,)),
    )
    yield DataSource(
        mimetype="multipart/related;type=image/tiff",
        assets=tiff_assets,
        structure_family=StructureFamily.array,
        structure=tiff_structure_0,
        management=Management.external,
    )


@pytest.fixture
def csv_data_source(csv_file):
    csv_assets = [
        Asset(
            data_uri=ensure_uri(csv_file),
            is_directory=False,
            parameter="data_uris",
        )
    ]
    yield DataSource(
        mimetype="text/csv",
        assets=csv_assets,
        structure_family=StructureFamily.table,
        structure=TableStructure.from_pandas(df3),
        management=Management.external,
    )


@pytest.mark.parametrize(
    "name, expected",
    [
        ("A", df1["A"]),
        ("B", df1["B"]),
        ("C", df2["C"]),
        ("D", df2["D"]),
        ("E", df2["E"]),
        ("arr1", arr1),
        ("arr2", arr2),
        ("awk", awk_arr),
        ("sps", sps_arr.todense()),
        ("rag", rag_arr),
    ],
)
def test_reading(context, name, expected):
    client = from_context(context)
    actual = client["x"][name].read()
    if name == "sps":
        actual = actual.todense()
    if name == "rag":
        # ragged.array equality is not array_equal-friendly; compare via awkward
        assert awkward.almost_equal(actual._impl, expected._impl)
        return
    assert numpy.array_equal(actual, expected)


def test_iterate_parts(context):
    client = from_context(context)
    for part in client["x"].base:
        client["x"].base[part].read()


def test_reading_dask(context):
    client = from_context(context, "dask")
    a = client["x"].read(["A"]).compute()
    assert df1["A"].equals(a.to_pandas()["A"])
    assert numpy.array_equal(
        arr1, client["x"].read(["arr1"]).compute()["arr1"].to_numpy()
    )


def test_iterate_columns(context):
    client = from_context(context)
    for col, _client in client["x"].items():
        read_from_client = _client.read()
        read_from_column = client["x"][col].read()
        if col == "sps":
            read_from_client = read_from_client.todense()
            read_from_column = read_from_column.todense()
        if col == "rag":
            assert awkward.almost_equal(read_from_client._impl, read_from_column._impl)
        else:
            assert numpy.array_equal(read_from_client, read_from_column)

        if col in ["arr1", "arr2", "awk", "sps", "rag"]:
            # Arrays can be read from the parent client
            assert client[f"x/{col}"].read() is not None
        else:
            # The column is not accessible from the parent client
            with pytest.raises(KeyError):
                client[f"x/{col}"].read()


def test_metadata(context):
    client = from_context(context)
    assert client["x"].metadata == md

    # Check metadata for each part
    for part in client["x"].base:
        c = client["x"].base[part]
        assert c.metadata["md_key"] == f"md_for_{part}"


def test_parts_not_directly_accessible(context):
    client = from_context(context)

    # The table is accessible as a part
    assert client["x"].base["df1"].read() is not None
    assert numpy.array_equal(client["x"].base["df1"]["A"].read(), df1["A"])
    assert numpy.array_equal(client["x"]["A"].read(), df1["A"])

    # The table is not accessible directly from the composite client
    with pytest.raises(KeyError):
        client["x"]["df1"].read()

    # The table is accessible from a parent client
    assert client["x/df1"].read() is not None
    assert client["x/df1/A"].read() is not None

    # The column is not accessible from the parent client
    with pytest.raises(KeyError):
        client["x/A"].read()


def test_external_assets(context, tiff_data_source, csv_data_source):
    client = from_context(context)

    y = client.create_container(key="y", specs=["composite"])
    y.new(
        structure_family=StructureFamily.array,
        data_sources=[tiff_data_source],
        key="image",
    )
    y.new(
        structure_family=StructureFamily.table,
        data_sources=[csv_data_source],
        key="table",
    )

    arr = y.base["image"].read()
    assert numpy.array_equal(arr, img_data)

    df = y.base["table"].read()
    for col in df.columns:
        assert numpy.array_equal(df[col], df3[col])

    assert set(y.keys()) == {"image", "col1", "col2"}


def test_read_full(context_for_reading):
    client = from_context(context_for_reading)
    ds = client["x"].read()

    assert isinstance(ds, xarray.Dataset)
    assert set(ds.data_vars) == {
        "arr1",
        "arr2",
        "A",
        "B",
        "C",
        "D",
        "E",
        "sps",
        "img",
        "col1",
        "col2",
        "rag",
    }


def test_read_selective(context_for_reading):
    client = from_context(context_for_reading)
    ds = client["x"].read(variables=["arr1", "arr2", "A", "B", "sps"])

    assert isinstance(ds, xarray.Dataset)
    assert set(ds.data_vars) == {"arr1", "arr2", "A", "B", "sps"}


@pytest.mark.parametrize("dim0", ["time", "col1"])
def test_read_selective_with_dim0(context_for_reading, dim0):
    client = from_context(context_for_reading)
    ds = client["x"].read(variables=["arr2", "img", "col1"], dim0=dim0)

    assert isinstance(ds, xarray.Dataset)
    assert set(ds.data_vars) == {"arr2", "img", "col1"}.difference([dim0])

    # Check the dimension names
    for var_name in ds.data_vars:
        assert ds[var_name].dims[0] == dim0


def test_read_jagged_ragged_pads(context):
    """A jagged (non-rectangular) ragged child cannot be converted to numpy
    directly; ``read()`` falls back to a NaN-padded dense array and warns."""
    client = from_context(context)
    with pytest.warns(UserWarning, match="Failed to convert ragged array"):
        ds = client["x"].read(variables=["rag"])
    assert "rag" in ds.data_vars
    # NaN-padded dense rectangular array
    values = ds["rag"].values
    assert values.ndim >= 2
    assert numpy.isnan(values).any()


def test_read_regular_ragged(context_for_reading):
    """A 'regular' (rectangular) ragged child is numpy-convertible and should
    appear in the xarray.Dataset returned by ``read()``."""
    client = from_context(context_for_reading)
    ds = client["x"].read(variables=["rag"])
    assert "rag" in ds.data_vars
    assert numpy.array_equal(ds["rag"].values, rag_regular._impl.to_numpy())


def test_delete_contents(context, tiff_data_source, csv_data_source):
    client = from_context(context)

    parts_before = len(client["x"].base)
    keys_before = len(client["x"].keys())
    assert parts_before == 7
    assert keys_before == 10

    # Attempt to delete an array that is internally managed
    with pytest.raises(ClientError):
        client["x"].delete_contents("arr1")

    # Delete a single part
    client["x"].delete_contents("arr1", external_only=False)
    assert "arr1" not in client["x"].base
    assert len(client["x"].base) == parts_before - 1
    assert len(client["x"].keys()) == keys_before - 1

    # Attempt to delete a DataFrame column
    assert "A" in client["x"].keys()
    with pytest.raises(KeyError):
        client["x"].delete_contents("A", external_only=False)
    client["x"].delete_contents("df1", external_only=False)
    assert "A" not in client["x"].keys()
    assert len(client["x"].base) == parts_before - 2
    assert len(client["x"].keys()) == keys_before - 3

    # Add and delete external data
    client["x"].new(
        structure_family=StructureFamily.array,
        data_sources=[tiff_data_source],
        key="image",
    )
    client["x"].new(
        structure_family=StructureFamily.table,
        data_sources=[csv_data_source],
        key="table",
    )
    # 6 original parts, 2 deleted, 2 added
    assert len(client["x"].base) == parts_before - 2 + 2
    assert len(client["x"].keys()) == keys_before - 3 + 3
    client["x"].delete_contents("image")
    assert "image" not in client["x"].base
    assert len(client["x"].base) == parts_before - 1
    assert len(client["x"].keys()) == keys_before - 1

    # Passing an empty list does not delete anything
    client["x"].delete_contents([])
    assert len(client["x"].base) == parts_before - 1
    assert len(client["x"].keys()) == keys_before - 1

    # Delete multiple parts
    client["x"].delete_contents(["arr2", "df2"], external_only=False)
    assert "arr2" not in client["x"].base
    assert "df1" not in client["x"].base

    # Delete all parts
    client["x"].delete_contents(external_only=False)
    assert len(client["x"].base) == 0
    assert len(client["x"].keys()) == 0


def test_write_one_table(client_for_writing):
    df = pandas.DataFrame({"A": [], "B": []})
    client_for_writing.create_container(key="z", specs=["composite"])
    client_for_writing["z"].write_table(df)
    assert len(client_for_writing["z"].base) == 1  # One table
    assert len(client_for_writing["z"]) == 2  # Two columns


def test_pagination(client_for_writing):
    df = pandas.DataFrame({"A": [], "B": []})
    z = client_for_writing.create_container(key="z", specs=["composite"])
    z.write_table(df)
    # Exercise pagination
    assert len(z.keys()[:1]) == 1
    assert len(z.values()[:1]) == 1
    assert len(z.items()[:1]) == 1


def test_write_dataframe_and_warn(client_for_writing):
    df = pandas.DataFrame({"A": [], "B": []})
    client_for_writing.create_container(key="z", specs=["composite"])
    with pytest.warns(DeprecationWarning):
        client_for_writing["z"].write_dataframe(df)
    assert len(client_for_writing["z"].base) == 1  # One table
    assert len(client_for_writing["z"]) == 2  # Two columns


def test_write_one_appendable_table(client_for_writing):
    client_for_writing.create_container(key="z", specs=["composite"])
    tab = pyarrow.Table.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    client_for_writing["z"].create_appendable_table(schema=tab.schema)
    assert len(client_for_writing["z"].base) == 1  # One table
    assert len(client_for_writing["z"]) == 2  # Two columns


def test_write_two_tables(client_for_writing):
    df1 = pandas.DataFrame({"A": [], "B": []})
    df2 = pandas.DataFrame({"C": [], "D": [], "E": []})
    z = client_for_writing.create_container(key="z", specs=["composite"])
    z.write_table(df1, key="table1")
    z.write_table(df2, key="table2")
    assert z.base["table1"].read() is not None
    assert z.base["table2"].read() is not None


def test_write_two_appendable_tables(client_for_writing):
    tab1 = pyarrow.Table.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    tab2 = pyarrow.Table.from_pydict({"C": [1, 2], "D": [3, 4], "E": [5, 6]})
    z = client_for_writing.create_container(key="z", specs=["composite"])
    tab1_client = z.create_appendable_table(schema=tab1.schema, key="table1")
    tab2_client = z.create_appendable_table(schema=tab2.schema, key="table2")
    tab1_client.append_partition(0, tab1)
    tab2_client.append_partition(0, tab2)
    assert z.base["table1"].read() is not None
    assert z.base["table2"].read() is not None


def test_write_two_tables_colliding_names(client_for_writing):
    df1 = pandas.DataFrame({"A": [], "B": []})
    df2 = pandas.DataFrame({"C": [], "D": [], "E": []})
    z = client_for_writing.create_container(key="z", specs=["composite"])
    z.write_table(df1, key="table1")
    with fail_with_status_code(HTTP_409_CONFLICT):
        z.write_table(df2, key="table1")


def test_write_two_appendable_tables_colliding_names(client_for_writing):
    tab1 = pyarrow.Table.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    tab2 = pyarrow.Table.from_pydict({"C": [1, 2], "D": [3, 4], "E": [5, 6]})
    z = client_for_writing.create_container(key="z", specs=["composite"])
    z.create_appendable_table(schema=tab1.schema, key="table1")
    with fail_with_status_code(HTTP_409_CONFLICT):
        z.create_appendable_table(schema=tab2.schema, key="table1")


def test_write_two_tables_colliding_keys(client_for_writing):
    df1 = pandas.DataFrame({"A": [], "B": []})
    df2 = pandas.DataFrame({"A": [], "C": [], "D": []})
    z = client_for_writing.create_container(key="z", specs=["composite"])
    z.write_table(df1, key="table1")
    with pytest.raises(ValueError):
        z.write_table(df2, key="table2")


def test_write_two_appendable_tables_colliding_keys(client_for_writing):
    tab1 = pyarrow.Table.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    tab2 = pyarrow.Table.from_pydict({"A": [1, 2], "C": [3, 4], "D": [5, 6]})
    z = client_for_writing.create_container(key="z", specs=["composite"])
    z.create_appendable_table(schema=tab1.schema, key="table1")
    with pytest.raises(ValueError):
        z.create_appendable_table(schema=tab2.schema, key="table2")


def test_write_two_tables_two_appendable_two_arrays(client_for_writing):
    df1 = pandas.DataFrame({"A": [], "B": []})
    df2 = pandas.DataFrame({"C": [], "D": [], "E": []})
    arr1 = numpy.ones((5, 5), dtype=numpy.float64)
    arr2 = 2 * numpy.ones((5, 5), dtype=numpy.int8)
    tab1 = pyarrow.Table.from_pydict({"F": [1, 2, 3], "G": [4, 5, 6]})
    tab2 = pyarrow.Table.from_pydict({"H": [1, 2], "I": [3, 4], "J": [5, 6]})
    z = client_for_writing.create_container(key="z", specs=["composite"])

    # Write by data source.
    z.write_table(df1, key="df1")
    z.write_table(df2, key="df2")
    z.write_array(arr1, key="arr1")
    z.write_array(arr2, key="arr2")
    tab1_client = z.create_appendable_table(schema=tab1.schema, key="tab1")
    tab2_client = z.create_appendable_table(schema=tab2.schema, key="tab2")
    tab1_client.append_partition(0, tab1)
    tab2_client.append_partition(0, tab2)

    # Read by data source.
    assert z.base["df1"].read() is not None
    assert z.base["df2"].read() is not None
    assert z.base["arr1"].read() is not None
    assert z.base["arr2"].read() is not None
    assert z.base["tab1"].read() is not None
    assert z.base["tab2"].read() is not None

    # Read by column.
    for column in {
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "I",
        "J",
        "arr1",
        "arr2",
    }:
        assert z[column].read() is not None


def test_write_table_column_array_key_collision(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        df = pandas.DataFrame({"A": [], "B": []})
        arr = numpy.array([1, 2, 3], dtype=numpy.float64)
        tab = pyarrow.Table.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})

        z1 = client.create_container(key="z1", specs=["composite"])
        z1.write_table(df, key="df1")
        with pytest.raises(ValueError):
            z1.write_array(arr, key="A")

        z2 = client.create_container(key="z2", specs=["composite"])
        z2.write_array(arr, key="A")
        with pytest.raises(ValueError):
            z2.write_table(df, key="df1")

        z3 = client.create_container(key="z3", specs=["composite"])
        z3.write_array(arr, key="A")
        with pytest.raises(ValueError):
            z3.create_appendable_table(schema=tab.schema, key="table1")

        z4 = client.create_container(key="z4", specs=["composite"])
        z4.write_table(df, key="df1")
        with pytest.raises(ValueError):
            z4.create_appendable_table(schema=tab.schema, key="table1")

        z5 = client.create_container(key="z5", specs=["composite"])
        z5.create_appendable_table(schema=tab.schema, key="table1")
        with pytest.raises(ValueError):
            z5.write_array(arr, key="A")

        z6 = client.create_container(key="z6", specs=["composite"])
        z6.create_appendable_table(schema=tab.schema, key="table1")
        with pytest.raises(ValueError):
            z6.write_table(df, key="df1")


def test_composite_validator(tree):
    "Test the spec validator for marking existing containers"
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        z = client.create_container(key="z")  # No specs

        # 1. Assign spec to an empty container
        z.update_metadata(specs=["composite"])
        assert Spec("composite") in z.specs
        assert isinstance(client["z"], CompositeClient)
        # Revert the assignment
        z.update_metadata(specs=[])
        assert Spec("composite") not in z.specs
        assert isinstance(client["z"], Container)

        # 2. Created a nested container
        z.create_container(key="z1")
        with pytest.raises(ClientError, match="Nested containers are not allowed"):
            z.update_metadata(specs=["composite"])
        z.delete_contents("z1")

        # 3. Write some initial array data
        z.write_array(arr1, key="arr1")
        z.write_array(arr2, key="arr2")
        z.write_awkward(awk_arr, key="awk")
        z.write_sparse(
            coords=sps_arr.coords,
            data=sps_arr.data,
            shape=sps_arr.shape,
            key="sps",
        )
        z.write_ragged(rag_arr, key="rag")

        # Composite spec can be assigned to a container with arrays
        z.update_metadata(specs=["composite"])
        assert Spec("composite") in z.specs
        assert isinstance(client["z"], CompositeClient)
        z.update_metadata(specs=[])

        # 4. Add two valid tables and two valid appendable tables
        z.write_table(df1, key="df1")
        z.write_table(df2, key="df2")
        z.create_appendable_table(schema=tab1.schema, key="tab1")
        z.create_appendable_table(schema=tab2.schema, key="tab2")

        # Composite spec can be assigned to a container with arrays and tables
        z.update_metadata(specs=["composite"])
        assert Spec("composite") in z.specs
        assert isinstance(client["z"], CompositeClient)
        z.update_metadata(specs=[])

        # 5. Add an array with a conflicting name
        z.write_array(arr1, key="A")
        with pytest.raises(ClientError, match="Found conflicting names"):
            z.update_metadata(specs=["composite"])
        z.delete_contents("A", external_only=False)

        # 6. Add tables with conflicting column names
        z.write_table(df1, key="df1_copy")
        with pytest.raises(ClientError, match="Found conflicting names"):
            z.update_metadata(specs=["composite"])
        z.delete_contents("df1_copy", external_only=False)
        z.create_appendable_table(schema=tab1.schema, key="tab1_copy")
        with pytest.raises(ClientError, match="Found conflicting names"):
            z.update_metadata(specs=["composite"])
        z.delete_contents("tab1_copy", external_only=False)

        # 7. Composite spec cannot be used for tables or arrays
        err_message = "Composite spec can be assigned only to containers"
        for key in ["arr1", "awk", "sps", "rag", "df1"]:
            with pytest.raises(ClientError, match=err_message):
                z[key].update_metadata(specs=["composite"])
        with pytest.raises(ClientError, match=err_message):
            z.write_table(df1, specs=["composite"])
        with pytest.raises(ClientError, match=err_message):
            z.write_array(arr1, specs=["composite"])
        with pytest.raises(ClientError, match=err_message):
            z.write_awkward(awk_arr, specs=["composite"])
        with pytest.raises(ClientError, match=err_message):
            z.write_sparse(
                coords=sps_arr.coords,
                data=sps_arr.data,
                shape=sps_arr.shape,
                specs=["composite"],
            )


# ---------------------------------------------------------------------------
# Progress bar total counting
# ---------------------------------------------------------------------------


def test_array_fetch_count(tmp_path):
    """ArrayClient.fetch_count() returns the correct number of HTTP requests.

    Small arrays (below RESPONSE_BYTESIZE_LIMIT) always need 1 request.
    Large arrays are split by split_slice, so fetch_count() > 1.
    We patch RESPONSE_BYTESIZE_LIMIT to a small value to exercise both paths
    without allocating large arrays.
    """
    from unittest.mock import patch

    from tiled.client.array import ArrayClient
    from tiled.utils import ensure_uri

    SMALL_LIMIT = 100  # bytes; (3,4)*float64=96B < limit, (10,10)*float64=800B > limit

    tree = in_memory(writable_storage=[ensure_uri(tmp_path / "data")])
    app = build_app(tree)
    with Context.from_app(app, show_progress=False) as ctx:
        client = from_context(ctx)
        client.write_array(rng.random((3, 4)), key="small")  # 96 B < 100
        client.write_array(rng.random((10, 10)), key="large")  # 800 B > 100

        with patch.object(ArrayClient, "RESPONSE_BYTESIZE_LIMIT", SMALL_LIMIT):
            assert client["small"].fetch_count() == 1
            assert client["large"].fetch_count() > 1


def test_composite_progress_total_mixed_small_and_large_arrays(tmp_path):
    """Progress bar total equals sum of fetch_count() across all arrays in a composite.

    Covers both small arrays (1 request each) and large arrays (multiple requests),
    ensuring the composite uses fetch_count() rather than dask chunk-boundary counts.
    We patch RESPONSE_BYTESIZE_LIMIT to 100 bytes so the large/small boundary is
    reachable with small test arrays.  Also verifies that when a subset of variables
    is requested, only those variables are counted.
    """
    from contextlib import contextmanager
    from unittest.mock import patch

    from tiled.client.array import ArrayClient
    from tiled.utils import ensure_uri

    SMALL_LIMIT = 100  # bytes; (3,4)*float64=96B < limit, (10,10)*float64=800B > limit

    tree = in_memory(writable_storage=[ensure_uri(tmp_path / "data")])
    app = build_app(tree)
    with Context.from_app(app, show_progress=False) as ctx:
        client = from_context(ctx)
        z = client.create_container(key="z", specs=["composite"])
        z.write_array(rng.random((3, 4)), key="small_a")  # 96 B → 1 request
        z.write_array(rng.random((3, 4)), key="small_b")  # 96 B → 1 request
        z.write_array(rng.random((10, 10)), key="large_a")  # 800 B → multiple requests
        z.write_array(rng.random((10, 10)), key="large_b")  # 800 B → multiple requests

        with patch.object(ArrayClient, "RESPONSE_BYTESIZE_LIMIT", SMALL_LIMIT):
            composite = from_context(ctx)["z"]

            def capture_outermost_total(variables=None):
                depth = [0]
                outermost_totals = []
                original_tp = ctx.tracking_progress

                @contextmanager
                def capturing_tp(total):
                    is_outermost = depth[0] == 0
                    depth[0] += 1
                    if is_outermost:
                        outermost_totals.append(total)
                    with original_tp(total) as s:
                        yield s
                    depth[0] -= 1

                with patch("tiled.client.utils.is_interactive", return_value=True):
                    with patch.object(ctx, "tracking_progress", capturing_tp):
                        ctx.show_progress = True
                        composite.read(variables=variables)
                        ctx.show_progress = False

                return outermost_totals

            # All four arrays
            all_keys = ("small_a", "small_b", "large_a", "large_b")
            expected_all = sum(composite.base[k].fetch_count() for k in all_keys)
            assert capture_outermost_total() == [expected_all]

            # Subset: one small, one large
            subset = ["small_a", "large_b"]
            expected_subset = sum(composite.base[k].fetch_count() for k in subset)
            assert capture_outermost_total(variables=subset) == [expected_subset]

            # Single variable
            assert capture_outermost_total(variables=["small_b"]) == [1]


# --- Bytes children are allowed in composite containers ---------------------------


@pytest.fixture(scope="function")
def composite_with_bytes(tmp_path):
    """A fresh composite container with one array and one bytes child."""
    from tiled.structures.bytes import BytesStructure

    catalog = in_memory(writable_storage=str(tmp_path / "store"))
    with Context.from_app(build_app(catalog)) as ctx:
        client = from_context(ctx)
        comp = client.create_container(key="c", specs=["composite"])
        comp.write_array(arr1, key="arr")
        # External bytes child registered manually.
        blob_path = tmp_path / "blob.bin"
        payload = b"opaque payload"
        blob_path.write_bytes(payload)
        comp.new(
            structure_family=StructureFamily.bytes,
            data_sources=[
                DataSource(
                    structure_family=StructureFamily.bytes,
                    structure=BytesStructure(),
                    mimetype="application/octet-stream",
                    management=Management.external,
                    assets=[
                        Asset(
                            data_uri=blob_path.as_uri(),
                            is_directory=False,
                            size=len(payload),
                            parameter="data_uris",
                            num=0,
                        )
                    ],
                )
            ],
            key="blob",
        )
        yield client, payload


def test_composite_lists_bytes_child(composite_with_bytes):
    """Bytes children appear in keys() and get_contents() like any other child."""
    client, _ = composite_with_bytes
    comp = client["c"]
    assert "arr" in comp.keys()
    assert "blob" in comp.keys()
    contents = comp.get_contents()
    assert contents["blob"]["attributes"]["structure_family"] == "bytes"


def test_composite_read_skips_bytes_children(composite_with_bytes):
    """Composite.read() returns a Dataset of array children only; bytes silently skipped."""
    client, _ = composite_with_bytes
    ds = client["c"].read()
    assert "arr" in ds.data_vars
    assert "blob" not in ds.data_vars
    numpy.testing.assert_array_equal(ds["arr"].values, arr1)


def test_composite_read_explicit_bytes_variable_raises(composite_with_bytes):
    """Explicitly asking for a bytes child by name raises a clear error."""
    client, _ = composite_with_bytes
    with pytest.raises(ValueError, match="bytes-family child 'blob'"):
        client["c"].read(variables=["blob"])


def test_composite_bytes_child_downloadable(composite_with_bytes):
    """Bytes child inside a composite is downloadable via /asset/bytes."""
    client, payload = composite_with_bytes
    meta = client.context.http_client.get(
        "/api/v1/metadata/c/blob", params={"include_data_sources": True}
    )
    asset_id = meta.json()["data"]["attributes"]["data_sources"][0]["assets"][0]["id"]
    response = client.context.http_client.get(
        "/api/v1/asset/bytes/c/blob", params={"id": asset_id}
    )
    assert response.status_code == 200
    assert response.content == payload
