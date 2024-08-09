import tempfile

import pyarrow as pa
import pytest

from tiled.adapters.arrow import ArrowAdapter
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
data_uri = "file://localhost/" + tempfile.gettempdir()


@pytest.fixture
def adapter() -> ArrowAdapter:
    table = pa.Table.from_arrays(data0, names)
    structure = TableStructure.from_arrow_table(table, npartitions=3)
    assets = ArrowAdapter.init_storage(data_uri, structure=structure)
    return ArrowAdapter([asset.data_uri for asset in assets], structure=structure)


def test_attributes(adapter: ArrowAdapter) -> None:
    assert adapter.structure().columns == names
    assert adapter.structure().npartitions == 3


def test_write_read(adapter: ArrowAdapter) -> None:
    # test writing to a partition and reading it
    adapter.write_partition(batch0, 0)
    assert pa.Table.from_arrays(data0, names) == pa.Table.from_pandas(
        adapter.read_partition(0)
    )

    adapter.write_partition([batch0, batch1], 1)
    assert pa.Table.from_batches([batch0, batch1]) == pa.Table.from_pandas(
        adapter.read_partition(1)
    )

    adapter.write_partition([batch0, batch1, batch2], 2)
    assert pa.Table.from_batches([batch0, batch1, batch2]) == pa.Table.from_pandas(
        adapter.read_partition(2)
    )

    # test write to all partitions and read all
    adapter.write_partition([batch0, batch1, batch2], 0)
    adapter.write_partition([batch2, batch0, batch1], 1)
    adapter.write_partition([batch1, batch2, batch0], 2)

    assert pa.Table.from_pandas(adapter.read()) == pa.Table.from_batches(
        [batch0, batch1, batch2, batch2, batch0, batch1, batch1, batch2, batch0]
    )

    # test adapter.write() raises NotImplementedError when there are more than 1 partitions
    with pytest.raises(NotImplementedError):
        adapter.write(batch0)
