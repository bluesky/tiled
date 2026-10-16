import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pytest

from tiled.adapters.arrow import ArrowAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.storage import FileStorage
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.data_source import DataSource, Management
from tiled.structures.table import TableStructure
from tiled.utils import APACHE_ARROW_FILE_MIME_TYPE, ensure_uri

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
def data_source_from_init_storage() -> DataSource[TableStructure]:
    table = pa.Table.from_arrays(data0, names)
    structure = TableStructure.from_arrow_table(table, npartitions=3)
    data_source = DataSource(
        management=Management.writable,
        mimetype="application/vnd.apache.arrow.file",
        structure_family=StructureFamily.table,
        structure=structure,
        assets=[],
    )
    storage = FileStorage(data_uri)
    return ArrowAdapter.init_storage(
        data_source=data_source, storage=storage, path_parts=[]
    )


@pytest.fixture
def adapter(data_source_from_init_storage: DataSource[TableStructure]) -> ArrowAdapter:
    data_source = data_source_from_init_storage
    return ArrowAdapter(
        [asset.data_uri for asset in data_source.assets],
        data_source.structure,
    )


def test_attributes(adapter: ArrowAdapter) -> None:
    assert adapter.structure().columns == names
    assert adapter.structure().npartitions == 3


def test_write_read(adapter: ArrowAdapter) -> None:
    # test writing to a partition and reading it
    adapter.write_partition(0, batch0)
    assert pa.Table.from_arrays(data0, names) == pa.Table.from_pandas(
        adapter.read_partition(0)
    )

    adapter.write_partition(1, [batch0, batch1])
    assert pa.Table.from_batches([batch0, batch1]) == pa.Table.from_pandas(
        adapter.read_partition(1)
    )

    adapter.write_partition(2, [batch0, batch1, batch2])
    assert pa.Table.from_batches([batch0, batch1, batch2]) == pa.Table.from_pandas(
        adapter.read_partition(2)
    )

    # test write to all partitions and read all
    adapter.write_partition(0, [batch0, batch1, batch2])
    adapter.write_partition(1, [batch2, batch0, batch1])
    adapter.write_partition(2, [batch1, batch2, batch0])

    assert pa.Table.from_pandas(adapter.read()) == pa.Table.from_batches(
        [batch0, batch1, batch2, batch2, batch0, batch1, batch1, batch2, batch0]
    )

    # test adapter.write() raises NotImplementedError when there are more than 1 partitions
    with pytest.raises(NotImplementedError):
        adapter.write(batch0)


def _write_arrow(path: Path, table: pa.Table) -> str:
    with pa.ipc.new_file(path, table.schema) as writer:
        writer.write_table(table)
    return ensure_uri(path)


@pytest.mark.parametrize(
    "table",
    [
        pa.Table.from_batches([batch0, batch1, batch2]),
        pa.Table.from_batches([], schema=batch0.schema),
        pa.table({"label": ["Si", "β-Ga₂O₃", "铜", None], "value": [1, 2, 3, 4]}),
    ],
    ids=["multiple-batches", "empty", "unicode-and-null"],
)
def test_infer_single_file_structure(tmp_path: Path, table: pa.Table) -> None:
    table = table.replace_schema_metadata({b"sample": b"semiconductor"})
    uri = _write_arrow(tmp_path / "table.arrow", table)
    metadata = {"sample": "Si"}
    specs = [Spec("test")]
    adapter = ArrowAdapter.from_single_file(uri, metadata=metadata, specs=specs)

    assert adapter.structure().npartitions == 1
    assert adapter.structure().columns == table.column_names
    assert adapter.structure().arrow_schema_decoded.equals(
        table.schema, check_metadata=True
    )
    assert adapter.metadata() == metadata
    assert adapter.specs == specs
    pd.testing.assert_frame_equal(adapter.read(), table.to_pandas())


@pytest.mark.parametrize("npartitions", [1, 3])
def test_infer_partitioned_structure(tmp_path: Path, npartitions: int) -> None:
    tables = [pa.Table.from_batches([batch]) for batch in (batch0, batch1, batch2)]
    uris = [
        _write_arrow(tmp_path / f"partition-{i}.arrow", table)
        for i, table in enumerate(tables[:npartitions])
    ]
    adapter = ArrowAdapter(uris)

    assert adapter.structure().npartitions == npartitions
    assert adapter.structure().columns == names
    for i, table in enumerate(tables[:npartitions]):
        pd.testing.assert_frame_equal(adapter.read_partition(i), table.to_pandas())
    pd.testing.assert_frame_equal(
        adapter.read(), pa.concat_tables(tables[:npartitions]).to_pandas()
    )


def test_infer_structure_ignores_partition_metadata(tmp_path: Path) -> None:
    table = pa.Table.from_batches([batch0])
    tables = [
        table.replace_schema_metadata({b"partition": str(i).encode()}) for i in range(2)
    ]
    adapter = ArrowAdapter(
        [_write_arrow(tmp_path / f"{i}.arrow", table) for i, table in enumerate(tables)]
    )
    assert adapter.structure().arrow_schema_decoded.equals(
        tables[0].schema, check_metadata=True
    )
    pd.testing.assert_frame_equal(adapter.read(), pa.concat_tables(tables).to_pandas())


def test_infer_structure_reads_only_schemas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = [tmp_path / f"{i}.arrow" for i in range(2)]
    uris = [_write_arrow(path, pa.Table.from_batches([batch0])) for path in paths]
    open_file = pa.ipc.open_file
    opened: list[Path] = []

    @contextmanager
    def schema_only_reader(path: Path) -> Iterator[SimpleNamespace]:
        opened.append(Path(path))
        with open_file(path) as reader:
            # Any attempt to read row data will fail on this schema-only handle.
            yield SimpleNamespace(schema=reader.schema)

    monkeypatch.setattr(pa.ipc, "open_file", schema_only_reader)
    adapter = ArrowAdapter(uris)
    assert adapter.structure().npartitions == 2
    assert opened == paths
    # Especially on Windows, this also verifies that reader handles are closed.
    for path in paths:
        path.unlink()


@pytest.mark.parametrize(
    "schema",
    [
        pa.schema([("renamed", pa.int64()), ("f1", pa.string()), ("f2", pa.bool_())]),
        pa.schema([("f0", pa.float64()), ("f1", pa.string()), ("f2", pa.bool_())]),
        pa.schema([("f1", pa.string()), ("f0", pa.int64()), ("f2", pa.bool_())]),
        pa.schema(
            [
                pa.field("f0", pa.int64(), nullable=False),
                ("f1", pa.string()),
                ("f2", pa.bool_()),
            ]
        ),
    ],
    ids=["names", "types", "order", "nullability"],
)
def test_infer_structure_rejects_mismatched_partitions(
    tmp_path: Path, schema: pa.Schema
) -> None:
    paths = [tmp_path / "first.arrow", tmp_path / "second.arrow"]
    uris = [
        _write_arrow(paths[0], pa.Table.from_batches([batch0])),
        _write_arrow(paths[1], schema.empty_table()),
    ]
    with pytest.raises(ValueError, match="same schema"):
        ArrowAdapter(uris)
    for path in paths:
        path.unlink()


def test_infer_structure_requires_partitions() -> None:
    with pytest.raises(ValueError, match="empty"):
        ArrowAdapter([])


def test_infer_structure_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="has not been stored yet"):
        ArrowAdapter.from_single_file(ensure_uri(tmp_path / "missing.arrow"))


def test_infer_structure_invalid_file(tmp_path: Path) -> None:
    path = tmp_path / "invalid.arrow"
    path.touch()
    with pytest.raises(pa.ArrowInvalid):
        ArrowAdapter.from_single_file(ensure_uri(path))


def test_explicit_structure_does_not_open_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_if_opened(*args: object, **kwargs: object) -> None:
        pytest.fail("Explicit structure should not cause files to be opened")

    monkeypatch.setattr(pa.ipc, "open_file", fail_if_opened)
    structure = TableStructure.from_schema(batch0.schema, npartitions=3)
    adapter = ArrowAdapter([ensure_uri(tmp_path / "missing.arrow")], structure)
    assert adapter.structure() is structure
    assert ArrowAdapter([], structure).structure() is structure


def test_inferred_structure_client_server_read(tmp_path: Path) -> None:
    table = pa.table({"label": ["Si", "β-Ga₂O₃", "铜"], "value": [1, 2, 3]})
    uri = _write_arrow(tmp_path / "table.arrow", table)
    adapter = ArrowAdapter.from_single_file(uri)
    # Context.from_app is an untyped upstream factory.
    with Context.from_app(build_app(MapAdapter({"table": adapter}))) as context:  # type: ignore[no-untyped-call]
        client = from_context(context)["table"]
        pd.testing.assert_frame_equal(
            client.read(), table.to_pandas(), check_dtype=False
        )
        assert client["label"].read().tolist() == table["label"].to_pylist()
        with pytest.raises(KeyError):
            client["missing"]


@pytest.mark.parametrize("npartitions", [1, 3])
@pytest.mark.parametrize("mimetype", [None, APACHE_ARROW_FILE_MIME_TYPE])
def test_register_existing_arrow_files(
    tmp_path: Path, npartitions: int, mimetype: str | None
) -> None:
    tables = [
        pa.table({"label": ["Si", "β-Ga₂O₃", "铜"], "value": [i, i + 1, i + 2]})
        for i in range(npartitions)
    ]
    uris = [
        _write_arrow(tmp_path / f"{i}.arrow", table) for i, table in enumerate(tables)
    ]
    catalog = in_memory(
        writable_storage=str(tmp_path / "data"), readable_storage=[str(tmp_path)]
    )
    with Context.from_app(build_app(catalog)) as context:  # type: ignore[no-untyped-call]
        client = from_context(context).include_data_sources()
        node = client.register(*uris, key="table", mimetype=mimetype)
        assert node.structure().npartitions == npartitions
        assert node.structure().arrow_schema_decoded.equals(
            tables[0].schema, check_metadata=True
        )
        # The client concatenates per-partition frames and may represent strings
        # with Dask's Arrow-backed dtype rather than pandas' object dtype.
        pd.testing.assert_frame_equal(
            node.read(),
            pd.concat([table.to_pandas() for table in tables]),
            check_dtype=False,
        )
        for i, table in enumerate(tables):
            pd.testing.assert_frame_equal(
                node.read_partition(i), table.to_pandas(), check_dtype=False
            )
        (data_source,) = node.data_sources()
        assert [asset.data_uri for asset in data_source.assets] == uris
        assert [asset.num for asset in data_source.assets] == list(range(npartitions))
        assert all(asset.parameter == "data_uris" for asset in data_source.assets)

        incompatible = pa.table({"label": ["Ge"], "value": [1.5]})
        bad_uri = _write_arrow(tmp_path / "bad.arrow", incompatible)
        with pytest.raises(ValueError, match="same schema"):
            client.register(uris[0], bad_uri, key="rejected", mimetype=mimetype)
        assert "rejected" not in client
        # The rejected import must not reserve the key or damage the valid node.
        retried = client.register(uris[0], key="rejected", mimetype=mimetype)
        pd.testing.assert_frame_equal(
            retried.read(), tables[0].to_pandas(), check_dtype=False
        )
