"""
This tests tiled's writing routes with an in-memory store.

Persistent stores are being developed externally to the tiled package.
"""
import base64
from datetime import datetime

import awkward
import dask.dataframe
import numpy
import pandas
import pandas.testing
import pytest
import sparse
from pandas.testing import assert_frame_equal
from starlette.status import (
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
    HTTP_422_UNPROCESSABLE_ENTITY,
)

from ..catalog import in_memory
from ..catalog.adapter import CatalogContainerAdapter
from ..client import Context, from_context, record_history
from ..mimetypes import PARQUET_MIMETYPE
from ..queries import Key
from ..server.app import build_app
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..structures.sparse import COOStructure
from ..structures.table import TableStructure
from ..utils import APACHE_ARROW_FILE_MIME_TYPE, patch_mimetypes
from ..validation_registration import ValidationRegistry
from .utils import fail_with_status_code

validation_registry = ValidationRegistry()
validation_registry.register("SomeSpec", lambda *args, **kwargs: None)


@pytest.fixture
def tree(tmpdir):
    return in_memory(writable_storage=tmpdir)


def test_write_array_full(tree):
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        a = numpy.ones((5, 7))

        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]
        with record_history() as history:
            client.write_array(a, metadata=metadata, specs=specs)
        # one request for metadata, one for data
        assert len(history.requests) == 1 + 1

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_array = result.read()

        numpy.testing.assert_equal(result_array, a)
        assert result.metadata == metadata
        assert result.specs == specs


def test_write_large_array_full(tree):
    "Test that a large array is chunked"
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        a = numpy.ones(100, dtype=numpy.uint8)
        # Low the limit so we can test on small data, for speed.
        original = client._SUGGESTED_MAX_UPLOAD_SIZE
        client._SUGGESTED_MAX_UPLOAD_SIZE = a.nbytes - 1
        try:
            metadata = {"scan_id": 1, "method": "A"}
            specs = [Spec("SomeSpec")]
            with record_history() as history:
                client.write_array(a, metadata=metadata, specs=specs)
            # one request for metadata, more than one for data
            assert len(history.requests) > 1 + 1

            results = client.search(Key("scan_id") == 1)
            result = results.values().first()
            result_array = result.read()

            numpy.testing.assert_equal(result_array, a)
            assert result.metadata == metadata
            assert result.specs == specs
        finally:
            client._SUGGESTED_MAX_UPLOAD_SIZE = original


def test_write_array_chunked(tree):
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        a = dask.array.arange(1500).reshape((50, 30)).rechunk((20, 15))

        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]
        with record_history() as history:
            client.write_array(a, metadata=metadata, specs=specs)
        # one request for metadata, multiple for data
        assert len(history.requests) == 1 + a.npartitions

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_array = result.read()

        numpy.testing.assert_equal(result_array, a.compute())
        assert result.metadata == metadata
        assert result.specs == specs


def test_write_dataframe_full(tree):
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        data = {f"Column{i}": (1 + i) * numpy.ones(5) for i in range(5)}
        df = pandas.DataFrame(data)
        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]

        with record_history() as history:
            client.write_dataframe(df, metadata=metadata, specs=specs)
        # one request for metadata, one for data
        assert len(history.requests) == 1 + 1

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_dataframe = result.read()

        pandas.testing.assert_frame_equal(result_dataframe, df)
        assert result.metadata == metadata
        assert result.specs == specs


def test_write_dataframe_partitioned(tree):
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        data = {f"Column{i}": (1 + i) * numpy.ones(10) for i in range(5)}
        df = pandas.DataFrame(data)
        ddf = dask.dataframe.from_pandas(df, npartitions=3)
        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]

        with record_history() as history:
            client.write_dataframe(ddf, metadata=metadata, specs=specs)
        # one request for metadata, multiple for data
        assert len(history.requests) == 1 + 3

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_dataframe = result.read()

        pandas.testing.assert_frame_equal(result_dataframe, df)
        assert result.metadata == metadata
        assert result.specs == specs


def test_write_dataframe_dict(tree):
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        data = {f"Column{i}": (1 + i) * numpy.ones(5) for i in range(5)}
        df = pandas.DataFrame(data)
        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]

        with record_history() as history:
            client.write_dataframe(data, metadata=metadata, specs=specs)
        # one request for metadata, one for data
        assert len(history.requests) == 1 + 1

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_dataframe = result.read()

        pandas.testing.assert_frame_equal(result_dataframe, df)
        assert result.metadata == metadata
        assert result.specs == specs


@pytest.mark.parametrize(
    "coo",
    [
        sparse.COO(coords=[[2, 5]], data=[1.3, 7.5], shape=(10,)),
        sparse.COO(coords=[[0, 1], [2, 3]], data=[3.8, 4.0], shape=(4, 4)),
    ],
)
def test_write_sparse_full(tree, coo):
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)
        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]
        with record_history() as history:
            client.write_sparse(
                coords=coo.coords,
                data=coo.data,
                shape=coo.shape,
                metadata=metadata,
                specs=specs,
            )
        # one request for metadata, one for data
        assert len(history.requests) == 1 + 1

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_array = result.read()

        numpy.testing.assert_equal(result_array.todense(), coo.todense())
        assert result.metadata == metadata
        assert result.specs == specs


def test_write_sparse_chunked(tree):
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]
        N = 5
        with record_history() as history:
            x = client.new(
                "sparse",
                [
                    DataSource(
                        structure=COOStructure(shape=(2 * N,), chunks=((N, N),)),
                        structure_family="sparse",
                    )
                ],
                metadata=metadata,
                specs=specs,
            )
            x.write_block(coords=[[2, 4]], data=[3.1, 2.8], block=(0,))
            x.write_block(coords=[[0, 1]], data=[6.7, 1.2], block=(1,))

        # one request for metadata, multiple for data
        assert len(history.requests) == 1 + 2

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_array = result.read()
        assert numpy.array_equal(
            result_array.todense(),
            sparse.COO(
                coords=[[2, 4, N + 0, N + 1]], data=[3.1, 2.8, 6.7, 1.2], shape=(10,)
            ).todense(),
        )

        # numpy.testing.assert_equal(result_array, sparse.COO(coords=[0, 1, ]))
        assert result.metadata == metadata
        assert result.specs == specs


def test_limits(tree):
    "Test various limits on uploaded metadata."

    MAX_ALLOWED_SPECS = 20
    MAX_SPEC_CHARS = 255

    validation_registry = ValidationRegistry()
    for i in range(101):
        validation_registry.register(f"spec{i}", lambda *args, **kwargs: None)
    validation_registry.register("one_too_many", lambda *args, **kwargs: None)
    validation_registry.register("a" * MAX_SPEC_CHARS, lambda *args, **kwargs: None)
    validation_registry.register(
        "a" * (1 + MAX_SPEC_CHARS), lambda *args, **kwargs: None
    )
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        # Up to 20 specs are allowed.
        max_allowed_specs = [f"spec{i}" for i in range(MAX_ALLOWED_SPECS)]
        x = client.write_array([1, 2, 3], specs=max_allowed_specs)
        x.update_metadata(specs=max_allowed_specs)  # no-op
        too_many_specs = max_allowed_specs + ["one_too_many"]
        with fail_with_status_code(HTTP_422_UNPROCESSABLE_ENTITY):
            client.write_array([1, 2, 3], specs=too_many_specs)
        with fail_with_status_code(HTTP_422_UNPROCESSABLE_ENTITY):
            x.update_metadata(specs=too_many_specs)

        # Specs cannot repeat.
        has_repeated_spec = ["spec0", "spec1", "spec0"]
        with fail_with_status_code(HTTP_422_UNPROCESSABLE_ENTITY):
            client.write_array([1, 2, 3], specs=has_repeated_spec)
        with fail_with_status_code(HTTP_422_UNPROCESSABLE_ENTITY):
            x.update_metadata(specs=has_repeated_spec)

        # A given spec cannot be too long.
        max_allowed_chars = ["a" * MAX_SPEC_CHARS]
        client.write_array([1, 2, 3], specs=max_allowed_chars)
        too_many_chars = ["a" * (1 + MAX_SPEC_CHARS)]
        with fail_with_status_code(HTTP_422_UNPROCESSABLE_ENTITY):
            client.write_array([1, 2, 3], specs=too_many_chars)
        with fail_with_status_code(HTTP_422_UNPROCESSABLE_ENTITY):
            x.update_metadata(specs=too_many_chars)


def test_metadata_revisions(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        ac = client.write_array([1, 2, 3], key="revise_me")
        assert len(ac.metadata_revisions[:]) == 0
        ac.update_metadata(metadata={"a": 1})
        assert ac.metadata["a"] == 1
        client["revise_me"].metadata["a"] == 1
        assert len(ac.metadata_revisions[:]) == 1
        ac.update_metadata(metadata={"a": 2})
        assert ac.metadata["a"] == 2
        client["revise_me"].metadata["a"] == 2
        assert len(ac.metadata_revisions[:]) == 2
        ac.metadata_revisions.delete_revision(1)
        assert len(ac.metadata_revisions[:]) == 1
        with fail_with_status_code(HTTP_404_NOT_FOUND):
            ac.metadata_revisions.delete_revision(1)


def test_merge_patching(tree):
    "Test merge patching of metadata and specs"
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        ac = client.write_array([1, 2, 3], metadata={"a": 0, "b": 2}, specs=["spec1"])
        ac.patch_metadata(
            metadata_patch={"a": 1, "c": 3}, content_type=patch_mimetypes.MERGE_PATCH
        )
        assert dict(ac.metadata) == {"a": 1, "b": 2, "c": 3}
        assert ac.specs[0].name == "spec1"
        ac.patch_metadata(
            specs_patch=["spec2"], content_type=patch_mimetypes.MERGE_PATCH
        )
        assert [x.name for x in ac.specs] == ["spec2"]


def test_json_patching(tree):
    "Test json patching of metadata and specs"

    validation_registry = ValidationRegistry()

    for i in range(10):
        validation_registry.register(f"spec{i}", lambda *args, **kwargs: None)

    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)
        ac = client.write_array([1, 2, 3], metadata={"a": 0, "b": 2}, specs=["spec1"])
        ac.patch_metadata(
            metadata_patch=[
                {"op": "add", "path": "/c", "value": 3},
                {"op": "replace", "path": "/a", "value": 1},
            ],
            content_type=patch_mimetypes.JSON_PATCH,
        )
        assert dict(ac.metadata) == {"a": 1, "b": 2, "c": 3}
        assert ac.specs[0].name == "spec1"
        ac.patch_metadata(
            specs_patch=[{"op": "add", "path": "/1", "value": "spec2"}],
            content_type=patch_mimetypes.JSON_PATCH,
        )
        assert [x.name for x in ac.specs] == ["spec1", "spec2"]


def test_metadata_with_unsafe_objects(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        ac = client.write_array(
            [1, 2, 3],
            metadata={"date": datetime.now(), "array": numpy.array([1, 2, 3])},
        )
        ac.metadata
        ac.read()


@pytest.mark.asyncio
async def test_delete(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        client.write_array(
            [1, 2, 3],
            metadata={"date": datetime.now(), "array": numpy.array([1, 2, 3])},
            key="x",
        )
        nodes_before_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_before_delete) == 1
        data_sources_before_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_before_delete) == 1
        assets_before_delete = (
            await tree.context.execute("SELECT * from assets")
        ).all()
        assert len(assets_before_delete) == 1

        # Writing again with the same name fails.
        with fail_with_status_code(HTTP_409_CONFLICT):
            client.write_array(
                [1, 2, 3],
                metadata={"date": datetime.now(), "array": numpy.array([1, 2, 3])},
                key="x",
            )

        client.delete("x")

        nodes_after_delete = (await tree.context.execute("SELECT * from nodes")).all()
        assert len(nodes_after_delete) == 0
        data_sources_after_delete = (
            await tree.context.execute("SELECT * from data_sources")
        ).all()
        assert len(data_sources_after_delete) == 0
        assets_after_delete = (await tree.context.execute("SELECT * from assets")).all()
        assert len(assets_after_delete) == 0

        # Writing again with the same name works now.
        client.write_array(
            [1, 2, 3],
            metadata={"date": datetime.now(), "array": numpy.array([1, 2, 3])},
            key="x",
        )


@pytest.mark.asyncio
async def test_delete_non_empty_node(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        a = client.create_container("a")
        b = a.create_container("b")
        c = b.create_container("c")
        d = c.create_container("d")

        # Cannot delete non-empty nodes
        assert "a" in client
        with fail_with_status_code(HTTP_409_CONFLICT):
            client.delete("a")
        assert "b" in a
        with fail_with_status_code(HTTP_409_CONFLICT):
            a.delete("b")
        assert "c" in b
        with fail_with_status_code(HTTP_409_CONFLICT):
            b.delete("c")
        assert "d" in c
        assert not list(d)  # leaf is empty
        # Delete from the bottom up.
        c.delete("d")
        b.delete("c")
        a.delete("b")
        client.delete("a")


@pytest.mark.asyncio
async def test_write_in_container(tree):
    "Create a container and write a structure into it."
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        a = client.create_container("a")
        df = pandas.DataFrame({"a": [1, 2, 3]})
        b = a.write_dataframe(df, key="b")
        b.read()
        a.delete("b")
        client.delete("a")

        a = client.create_container("a")
        arr = numpy.array([1, 2, 3])
        b = a.write_array(arr, key="b")
        b.read()
        a.delete("b")
        client.delete("a")

        a = client.create_container("a")
        coo = sparse.COO(coords=[[2, 5]], data=[1.3, 7.5], shape=(10,))
        b = a.write_sparse(coords=coo.coords, data=coo.data, shape=coo.shape, key="b")
        b.read()
        a.delete("b")
        client.delete("a")

        a = client.create_container("a")
        array = awkward.Array(
            [
                [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
                [],
                [{"x": 3.3, "y": [1, 2, 3]}],
            ]
        )
        b = a.write_awkward(array, key="b")
        b.read()
        a.delete("b")
        client.delete("a")


@pytest.mark.asyncio
async def test_bytes_in_metadata(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        client.create_container("a", metadata={"test": b"raw_bytes"})
        value = client["a"].metadata["test"]
        assert value.startswith("data:application/octet-stream;base64,")
        label, encoded = value.split(",", 1)
        assert base64.b64decode(encoded) == b"raw_bytes"


@pytest.mark.asyncio
async def test_container_export(tree, buffer):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        a = client.create_container("a")
        a.write_array([1, 2, 3], key="b")
        client.export(buffer, format="application/json")


def test_write_with_specified_mimetype(tree):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context, include_data_sources=True)
        df = pandas.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        structure = TableStructure.from_pandas(df)

        for mimetype in [PARQUET_MIMETYPE, "text/csv", APACHE_ARROW_FILE_MIME_TYPE]:
            x = client.new(
                "table",
                [
                    DataSource(
                        structure_family=StructureFamily.table,
                        structure=structure,
                        mimetype=mimetype,
                    ),
                ],
            )
            x.write_partition(df, 0)
            x.read()
            x.refresh()
            assert x.data_sources()[0].mimetype == mimetype

        # Specifying unsupported mimetype raises expected error.
        with fail_with_status_code(415):
            client.new(
                "table",
                [
                    DataSource(
                        structure_family=StructureFamily.table,
                        structure=structure,
                        mimetype="application/x-does-not-exist",
                    ),
                ],
            )


@pytest.mark.parametrize(
    "orig_file, file_toappend, expected_file",
    [
        (
            {"A": [1, 2, 3], "B": [4, 5, 6]},
            {"A": [11, 12, 13], "B": [14, 15, 16]},
            {"A": [1, 2, 3, 11, 12, 13], "B": [4, 5, 6, 14, 15, 16]},
        ),
        (
            {"A": [1.2, 2.5, 3.7], "B": [4.6, 5.8, 6.9]},
            {"A": [11.2, 12.5, 13.7], "B": [14.6, 15.8, 16.9]},
            {
                "A": [1.2, 2.5, 3.7, 11.2, 12.5, 13.7],
                "B": [4.6, 5.8, 6.9, 14.6, 15.8, 16.9],
            },
        ),
        (
            {"C": ["x", "y"], "D": ["a", "b"]},
            {"C": ["xx", "yy", "zz"], "D": ["aa", "bb", "cc"]},
            {"C": ["x", "y", "xx", "yy", "zz"], "D": ["a", "b", "aa", "bb", "cc"]},
        ),
    ],
)
def test_append_partition(
    tree: CatalogContainerAdapter,
    orig_file: dict,
    file_toappend: dict,
    expected_file: dict,
):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context, include_data_sources=True)
        df = pandas.DataFrame(orig_file)
        structure = TableStructure.from_pandas(df)

        x = client.new(
            "table",
            [
                DataSource(
                    structure_family="table",
                    structure=structure,
                    mimetype="text/csv",
                ),
            ],
            key="x",
        )
        x.write(df)

        df2 = pandas.DataFrame(file_toappend)

        x.append_partition(df2, 0)

        df3 = pandas.DataFrame(expected_file)

        assert_frame_equal(x.read(), df3, check_dtype=False)
