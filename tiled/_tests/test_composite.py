import awkward
import numpy
import pandas
import pytest
import sparse
import tifffile as tf
import xarray
from starlette.status import HTTP_409_CONFLICT

from ..catalog import in_memory
from ..client import Context, from_context
from ..client.composite import CompositeClient
from ..client.container import Container
from ..client.utils import ClientError
from ..server.app import build_app
from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..structures.table import TableStructure
from ..utils import ensure_uri
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
        x.write_dataframe(
            df1,
            key="df1",
            metadata={
                "md_key": "md_for_df1",
                "A": {"md_key": "md_for_A"},
                "B": {"md_key": "md_for_B"},
            },
        )
        x.write_dataframe(
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

        yield context


@pytest.fixture(scope="function")
def context_for_read(context):
    client = from_context(context)

    # Awkward arrays are not supported when building xarray, in general
    client["x"].delete_contents("awk", external_only=False)
    # Add an image array and a table with 5 rows
    client["x"].write_array(img_data, key="img")
    client["x"].write_dataframe(df3, key="df3")

    yield context

    # Restore the original context
    client["x"].write_awkward(awk_arr, key="awk", metadata={"md_key": "md_for_awk"})
    client["x"].delete_contents("img", external_only=False)
    client["x"].delete_contents("df3", external_only=False)


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
    ],
)
def test_reading(context, name, expected):
    client = from_context(context)
    actual = client["x"][name].read()
    if name == "sps":
        actual = actual.todense()
    assert numpy.array_equal(actual, expected)


def test_iterate_parts(context):
    client = from_context(context)
    for part in client["x"].base:
        client["x"].base[part].read()


def test_iterate_columns(context):
    client = from_context(context)
    for col, _client in client["x"].items():
        read_from_client = _client.read()
        read_from_column = client["x"][col].read()
        if col == "sps":
            read_from_client = read_from_client.todense()
            read_from_column = read_from_column.todense()
        assert numpy.array_equal(read_from_client, read_from_column)

        if col in ["arr1", "arr2", "awk", "sps"]:
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


def test_read_full(context_for_read):
    client = from_context(context_for_read)
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
    }


def test_read_selective(context_for_read):
    client = from_context(context_for_read)
    ds = client["x"].read(variables=["arr1", "arr2", "A", "B", "sps"])

    assert isinstance(ds, xarray.Dataset)
    assert set(ds.data_vars) == {"arr1", "arr2", "A", "B", "sps"}


@pytest.mark.parametrize("dim0", ["time", "col1"])
def test_read_selective_with_dim0(context_for_read, dim0):
    client = from_context(context_for_read)
    ds = client["x"].read(variables=["arr2", "img", "col1"], dim0=dim0)

    assert isinstance(ds, xarray.Dataset)
    assert set(ds.data_vars) == {"arr2", "img", "col1"}.difference([dim0])

    # Check the dimension names
    for var_name in ds.data_vars:
        assert ds[var_name].dims[0] == dim0


def test_delete_contents(context, tiff_data_source, csv_data_source):
    client = from_context(context)

    parts_before = len(client["x"].base)
    keys_before = len(client["x"].keys())
    assert parts_before == 6
    assert keys_before == 9

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


def test_write_one_table(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        df = pandas.DataFrame({"A": [], "B": []})
        client.create_container(key="z", specs=["composite"])
        client["z"].write_dataframe(df)
        assert len(client["z"].base) == 1  # One table
        assert len(client["z"]) == 2  # Two columns


def test_write_two_tables(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        df1 = pandas.DataFrame({"A": [], "B": []})
        df2 = pandas.DataFrame({"C": [], "D": [], "E": []})
        z = client.create_container(key="z", specs=["composite"])
        z.write_dataframe(df1, key="table1")
        z.write_dataframe(df2, key="table2")
        z.base["table1"].read()
        z.base["table2"].read()


def test_write_two_tables_colliding_names(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        df1 = pandas.DataFrame({"A": [], "B": []})
        df2 = pandas.DataFrame({"C": [], "D": [], "E": []})
        z = client.create_container(key="z", specs=["composite"])
        z.write_dataframe(df1, key="table1")
        with fail_with_status_code(HTTP_409_CONFLICT):
            z.write_dataframe(df2, key="table1")


def test_write_two_tables_colliding_keys(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        df1 = pandas.DataFrame({"A": [], "B": []})
        df2 = pandas.DataFrame({"A": [], "C": [], "D": []})
        z = client.create_container(key="z", specs=["composite"])
        z.write_dataframe(df1, key="table1")
        with pytest.raises(ValueError):
            z.write_dataframe(df2, key="table2")


def test_write_two_tables_two_arrays(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        df1 = pandas.DataFrame({"A": [], "B": []})
        df2 = pandas.DataFrame({"C": [], "D": [], "E": []})
        arr1 = numpy.ones((5, 5), dtype=numpy.float64)
        arr2 = 2 * numpy.ones((5, 5), dtype=numpy.int8)
        z = client.create_container(key="z", specs=["composite"])

        # Write by data source.
        z.write_dataframe(df1, key="table1")
        z.write_dataframe(df2, key="table2")
        z.write_array(arr1, key="F")
        z.write_array(arr2, key="G")

        # Read by data source.
        z.base["table1"].read()
        z.base["table2"].read()
        z.base["F"].read()
        z.base["G"].read()

        # Read by column.
        for column in ["A", "B", "C", "D", "E", "F", "G"]:
            z[column].read()


def test_write_table_column_array_key_collision(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        df = pandas.DataFrame({"A": [], "B": []})
        arr = numpy.array([1, 2, 3], dtype=numpy.float64)

        z1 = client.create_container(key="z1", specs=["composite"])
        z1.write_dataframe(df, key="table1")
        with pytest.raises(ValueError):
            z1.write_array(arr, key="A")

        z2 = client.create_container(key="z2", specs=["composite"])
        z2.write_array(arr, key="A")
        with pytest.raises(ValueError):
            z2.write_dataframe(df, key="table1")


def test_composite_validator(tree):
    "Test the spec validator for marking existing containers"
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        y = client.create_container(key="y")  # No specs

        # 1. Assign spec to an empty container
        y.update_metadata(specs=["composite"])
        assert Spec("composite") in y.specs
        assert isinstance(client["y"], CompositeClient)
        # Revert the assignment
        y.update_metadata(specs=[])
        assert Spec("composite") not in y.specs
        assert isinstance(client["y"], Container)

        # 2. Created a nested container
        y.create_container(key="z")
        with pytest.raises(ClientError, match="Nested containers are not allowed"):
            y.update_metadata(specs=["composite"])
        y.delete_contents("z")

        # 3. Write some initial array data
        y.write_array(arr1, key="arr1")
        y.write_array(arr2, key="arr2")
        y.write_awkward(awk_arr, key="awk")
        y.write_sparse(
            coords=sps_arr.coords,
            data=sps_arr.data,
            shape=sps_arr.shape,
            key="sps",
        )

        # Composite spec can be assigned to a container with arrays
        y.update_metadata(specs=["composite"])
        assert Spec("composite") in y.specs
        assert isinstance(client["y"], CompositeClient)
        y.update_metadata(specs=[])

        # 4. Add two valid tables
        y.write_dataframe(df1, key="df1")
        y.write_dataframe(df2, key="df2")

        # Composite spec can be assigned to a container with arrays and tables
        y.update_metadata(specs=["composite"])
        assert Spec("composite") in y.specs
        assert isinstance(client["y"], CompositeClient)
        y.update_metadata(specs=[])

        # 5. Add an array with a conflicting name
        y.write_array(arr1, key="A")
        with pytest.raises(ClientError, match="Found conflicting names"):
            y.update_metadata(specs=["composite"])
        y.delete_contents("A", external_only=False)

        # 6. Add a table with a conflicting column names
        y.write_dataframe(df1, key="df1_copy")
        with pytest.raises(ClientError, match="Found conflicting names"):
            y.update_metadata(specs=["composite"])
        y.delete_contents("df1_copy", external_only=False)

        # 7. Composite spec cannot be used for tables or arrays
        err_message = "Composite spec can be assigned only to containers"
        for key in ["arr1", "awk", "sps", "df1"]:
            with pytest.raises(ClientError, match=err_message):
                y[key].update_metadata(specs=["composite"])
        with pytest.raises(ClientError, match=err_message):
            y.write_dataframe(df1, specs=["composite"])
        with pytest.raises(ClientError, match=err_message):
            y.write_array(arr1, specs=["composite"])
        with pytest.raises(ClientError, match=err_message):
            y.write_awkward(awk_arr, specs=["composite"])
        with pytest.raises(ClientError, match=err_message):
            y.write_sparse(
                coords=sps_arr.coords,
                data=sps_arr.data,
                shape=sps_arr.shape,
                specs=["composite"],
            )
