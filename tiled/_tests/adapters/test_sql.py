import tempfile

import adbc_driver_sqlite
import pyarrow as pa
import pytest

from tiled.adapters.sql import SQLAdapter
from tiled.structures.table import TableStructure

names = ["f0", "f1", "f2"]
data0 = [
    pa.array([1, 2, 3, 4, 5]),
    pa.array(["foo0", "bar0", "baz0", None, "goo0"]),
    pa.array([True, None, False, True, None]),
]
data1 = [
    pa.array([6, 7, 8, 9, 10, 11, 12]),
    pa.array(["foo1", "bar1", None, "baz1", "biz", None, "goo"]),
    pa.array([None, True, True, False, False, None, True]),
]
data2 = [pa.array([13, 14]), pa.array(["foo2", "baz2"]), pa.array([False, None])]

batch0 = pa.record_batch(data0, names=names)
batch1 = pa.record_batch(data1, names=names)
batch2 = pa.record_batch(data2, names=names)


def test_invalid_uri() -> None:
    data_uri = "/some_random_uri/test.db"
    table = pa.Table.from_arrays(data0, names)
    structure = TableStructure.from_arrow_table(table, npartitions=1)
    asset = SQLAdapter.init_storage(data_uri, structure=structure)
    with pytest.raises(
        ValueError,
        match="The database uri must start with either `sqlite://` or `postgresql://` ",
    ):
        SQLAdapter(asset.data_uri, structure=structure)


def test_invalid_structure() -> None:
    data_uri = "/some_random_uri/test.db"
    table = pa.Table.from_arrays(data0, names)
    structure = TableStructure.from_arrow_table(table, npartitions=3)
    with pytest.raises(ValueError, match="The SQL adapter must have only 1 partition"):
        SQLAdapter.init_storage(data_uri, structure=structure)


@pytest.fixture
def adapter() -> SQLAdapter:
    data_uri = "sqlite://file://localhost" + tempfile.gettempdir() + "/test.db"
    table = pa.Table.from_arrays(data0, names)
    structure = TableStructure.from_arrow_table(table, npartitions=1)
    asset = SQLAdapter.init_storage(data_uri, structure=structure)
    return SQLAdapter(asset.data_uri, structure=structure)


def test_attributes(adapter: SQLAdapter) -> None:
    assert adapter.structure().columns == names
    assert adapter.structure().npartitions == 1
    assert isinstance(adapter.conn, adbc_driver_sqlite.dbapi.AdbcSqliteConnection)


def test_write_read(adapter: SQLAdapter) -> None:
    # test writing to a partition and reading it
    adapter.write(batch0)
    result = adapter.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result["f2"] = result["f2"].astype("boolean")

    assert pa.Table.from_arrays(data0, names) == pa.Table.from_pandas(result)

    adapter.write([batch0, batch1])
    result = adapter.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result["f2"] = result["f2"].astype("boolean")
    assert pa.Table.from_batches([batch0, batch1]) == pa.Table.from_pandas(result)

    adapter.write([batch0, batch1, batch2])
    result = adapter.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result["f2"] = result["f2"].astype("boolean")
    assert pa.Table.from_batches([batch0, batch1, batch2]) == pa.Table.from_pandas(
        result
    )

    # test write to all partitions and read all
    adapter.write([batch0, batch1, batch2])
    adapter.append([batch2, batch0, batch1])
    adapter.append([batch1, batch2, batch0])
    result = adapter.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result["f2"] = result["f2"].astype("boolean")

    assert pa.Table.from_batches(
        [batch0, batch1, batch2, batch2, batch0, batch1, batch1, batch2, batch0]
    ) == pa.Table.from_pandas(result)
