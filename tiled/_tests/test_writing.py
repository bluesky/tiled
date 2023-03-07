"""
This tests tiled's writing routes with an in-memory store.

Persistent stores are being developed externally to the tiled package.
"""

import dask.dataframe
import numpy
import pandas.testing
import sparse

from ..client import Context, from_context, record_history
from ..queries import Key
from ..server.app import build_app
from ..structures.core import Spec
from ..structures.sparse import COOStructure
from ..validation_registration import ValidationRegistry
from .utils import fail_with_status_code
from .writable_adapters import WritableMapAdapter

validation_registry = ValidationRegistry()
validation_registry.register("SomeSpec", lambda *args, **kwargs: None)


def test_write_array_full():

    tree = WritableMapAdapter({})
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        a = numpy.ones((5, 7))

        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]
        references = [{"label": "test", "url": "http://www.test.com"}]
        with record_history() as history:
            client.write_array(a, metadata=metadata, specs=specs, references=references)
        # one request for metadata, one for data
        assert len(history.requests) == 1 + 1

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_array = result.read()

        numpy.testing.assert_equal(result_array, a)
        assert result.metadata == metadata
        assert result.specs == specs
        assert result.references == references


def test_write_large_array_full():
    "Test that a large array is chunked"

    tree = WritableMapAdapter({})
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
            references = [{"label": "test", "url": "http://www.test.com"}]
            with record_history() as history:
                client.write_array(
                    a, metadata=metadata, specs=specs, references=references
                )
            # one request for metadata, more than one for data
            assert len(history.requests) > 1 + 1

            results = client.search(Key("scan_id") == 1)
            result = results.values().first()
            result_array = result.read()

            numpy.testing.assert_equal(result_array, a)
            assert result.metadata == metadata
            assert result.specs == specs
            assert result.references == references
        finally:
            client._SUGGESTED_MAX_UPLOAD_SIZE = original


def test_write_array_chunked():

    tree = WritableMapAdapter({})
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        a = dask.array.arange(1500).reshape((50, 30)).rechunk((20, 15))

        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]
        references = [{"label": "test", "url": "http://www.test.com"}]
        with record_history() as history:
            client.write_array(a, metadata=metadata, specs=specs, references=references)
        # one request for metadata, multiple for data
        assert len(history.requests) == 1 + a.npartitions

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_array = result.read()

        numpy.testing.assert_equal(result_array, a.compute())
        assert result.metadata == metadata
        assert result.specs == specs
        assert result.references == references


def test_write_dataframe_full():

    tree = WritableMapAdapter({})
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        data = {f"Column{i}": (1 + i) * numpy.ones(5) for i in range(5)}
        df = pandas.DataFrame(data)
        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]
        references = [{"label": "test", "url": "http://www.test.com"}]

        with record_history() as history:
            client.write_dataframe(
                df, metadata=metadata, specs=specs, references=references
            )
        # one request for metadata, one for data
        assert len(history.requests) == 1 + 1

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_dataframe = result.read()

        pandas.testing.assert_frame_equal(result_dataframe, df)
        assert result.metadata == metadata
        assert result.specs == specs
        assert result.references == references


def test_write_dataframe_partitioned():

    tree = WritableMapAdapter({})
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        data = {f"Column{i}": (1 + i) * numpy.ones(10) for i in range(5)}
        df = pandas.DataFrame(data)
        ddf = dask.dataframe.from_pandas(df, npartitions=3)
        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]
        references = [{"label": "test", "url": "http://www.test.com"}]

        with record_history() as history:
            client.write_dataframe(
                ddf, metadata=metadata, specs=specs, references=references
            )
        # one request for metadata, multiple for data
        assert len(history.requests) == 1 + 3

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_dataframe = result.read()

        pandas.testing.assert_frame_equal(result_dataframe, df)
        assert result.metadata == metadata
        assert result.specs == specs
        assert result.references == references


def test_write_sparse_full():

    tree = WritableMapAdapter({})
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        coo = sparse.COO(coords=[[0, 1], [2, 3]], data=[3.8, 4.0], shape=(4, 4))

        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]
        references = [{"label": "test", "url": "http://www.test.com"}]
        with record_history() as history:
            client.write_sparse(
                coords=coo.coords,
                data=coo.data,
                shape=coo.shape,
                metadata=metadata,
                specs=specs,
                references=references,
            )
        # one request for metadata, one for data
        assert len(history.requests) == 1 + 1

        results = client.search(Key("scan_id") == 1)
        result = results.values().first()
        result_array = result.read()

        numpy.testing.assert_equal(result_array.todense(), coo.todense())
        assert result.metadata == metadata
        assert result.specs == specs
        assert result.references == references


def test_write_sparse_chunked():

    tree = WritableMapAdapter({})
    with Context.from_app(
        build_app(tree, validation_registry=validation_registry)
    ) as context:
        client = from_context(context)

        metadata = {"scan_id": 1, "method": "A"}
        specs = [Spec("SomeSpec")]
        references = [{"label": "test", "url": "http://www.test.com"}]
        N = 5
        with record_history() as history:
            x = client.new(
                "sparse",
                COOStructure(shape=(2 * N,), chunks=((N, N),)),
                metadata=metadata,
                specs=specs,
                references=references,
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
        assert result.references == references


def test_limits():
    "Test various limits on uploaded metadata."

    MAX_ALLOWED_SPECS = 20
    MAX_SPEC_CHARS = 255
    MAX_ALLOWED_REFERENCES = 20
    MAX_LABEL_CHARS = 255

    tree = WritableMapAdapter({})
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
        with fail_with_status_code(422):
            client.write_array([1, 2, 3], specs=too_many_specs)
        with fail_with_status_code(422):
            x.update_metadata(specs=too_many_specs)

        # Specs cannot repeat.
        has_repeated_spec = ["spec0", "spec1", "spec0"]
        with fail_with_status_code(422):
            client.write_array([1, 2, 3], specs=has_repeated_spec)
        with fail_with_status_code(422):
            x.update_metadata(specs=has_repeated_spec)

        # A given spec cannot be too long.
        max_allowed_chars = ["a" * MAX_SPEC_CHARS]
        client.write_array([1, 2, 3], specs=max_allowed_chars)
        too_many_chars = ["a" * (1 + MAX_SPEC_CHARS)]
        with fail_with_status_code(422):
            client.write_array([1, 2, 3], specs=too_many_chars)
        with fail_with_status_code(422):
            x.update_metadata(specs=too_many_chars)

        # Up to 20 references are allowed.
        max_allowed_references = [
            {"label": f"ref{i}", "url": f"https://exmaple.com/{i}"}
            for i in range(MAX_ALLOWED_REFERENCES)
        ]
        y = client.write_array([1, 2, 3], references=max_allowed_references)
        y.update_metadata(references=max_allowed_references)  # no-op
        too_many_references = max_allowed_references + [
            {"label": "one_too_many", "url": "https://example.com/one_too_many"}
        ]
        with fail_with_status_code(422):
            client.write_array([1, 2, 3], references=too_many_references)
        with fail_with_status_code(422):
            y.update_metadata(references=too_many_references)

        # A given reference label cannot be too long.
        max_allowed_chars = "a" * MAX_LABEL_CHARS
        client.write_array(
            [1, 2, 3],
            references=[{"label": max_allowed_chars, "url": "https://example.com"}],
        )
        too_many_chars = max_allowed_chars + "a"
        with fail_with_status_code(422):
            client.write_array(
                [1, 2, 3],
                references=[{"label": too_many_chars, "url": "https://example.com"}],
            )
        with fail_with_status_code(422):
            y.update_metadata(references=too_many_chars)
