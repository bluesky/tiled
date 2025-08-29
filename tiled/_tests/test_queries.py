import asyncio
import string
import subprocess
import sys
from contextlib import nullcontext

import numpy
import pytest
import pytest_asyncio
from starlette.status import HTTP_400_BAD_REQUEST

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..catalog import from_uri, in_memory
from ..client import Context, from_context
from ..queries import (
    Comparison,
    Contains,
    Eq,
    FullText,
    In,
    Key,
    KeyPresent,
    KeysFilter,
    Like,
    NotEq,
    NotIn,
    Regex,
    SpecsQuery,
    StructureFamilyQuery,
)
from ..server.app import build_app
from .conftest import TILED_TEST_POSTGRESQL_URI
from .utils import fail_with_status_code, sqlite_from_dump, temp_postgres

keys = list(string.ascii_lowercase)
mapping = {
    letter: ArrayAdapter.from_array(
        number * numpy.ones(10),
        metadata={"letter": letter, "number": number},
        specs=[letter],
    )
    for letter, number in zip(keys, range(26))
}
mapping["does_contain_z"] = ArrayAdapter.from_array(
    numpy.ones(10), metadata={"letters": list(string.ascii_lowercase)}
)
mapping["does_not_contain_z"] = ArrayAdapter.from_array(
    numpy.ones(10), metadata={"letters": list(string.ascii_lowercase[:-1])}
)
mapping["full_text_test_case"] = ArrayAdapter.from_array(
    numpy.ones(10), metadata={"color": "purple"}
)

mapping["full_text_test_case_urple"] = ArrayAdapter.from_array(
    numpy.ones(10), metadata={"color": "urple"}
)

mapping["specs_foo_bar"] = ArrayAdapter.from_array(numpy.ones(10), specs=["foo", "bar"])
mapping["specs_foo_bar_baz"] = ArrayAdapter.from_array(
    numpy.ones(10), specs=["foo", "bar", "baz"]
)
nested_metadata = {
    "nested": {"nested-key-1": "nested-value-1", "nested-key-2": "nested-value-2"}
}
mapping["nested_key_test"] = ArrayAdapter.from_array(
    numpy.ones(10), metadata=nested_metadata
)


@pytest.fixture(scope="module")
def event_loop():
    # https://stackoverflow.com/a/56238383/1221924
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="module", params=["map", "sqlite", "postgresql"])
async def client(request, tmpdir_module):
    if request.param == "map":
        tree = MapAdapter(mapping, metadata={"backend": request.param})
        app = build_app(tree)
        with Context.from_app(app) as context:
            client = from_context(context)
            yield client
    elif request.param == "sqlite":
        tree = in_memory(
            writable_storage=str(tmpdir_module / "sqlite"),
            metadata={"backend": request.param},
        )
        app = build_app(tree)
        with Context.from_app(app) as context:
            client = from_context(context)
            for k, v in mapping.items():
                client.write_array(
                    v.read(), key=k, metadata=dict(v.metadata()), specs=v.specs
                )
            yield client
    elif request.param == "postgresql":
        if not TILED_TEST_POSTGRESQL_URI:
            raise pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
        # Create temporary database.
        async with temp_postgres(TILED_TEST_POSTGRESQL_URI) as uri_with_database_name:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tiled",
                    "catalog",
                    "init",
                    uri_with_database_name,
                ],
                check=True,
                capture_output=True,
            )
            tree = from_uri(
                uri_with_database_name,
                writable_storage=str(tmpdir_module / "postgresql"),
                metadata={"backend": request.param},
            )
            app = build_app(tree)
            with Context.from_app(app) as context:
                client = from_context(context)
                # Write data into catalog.
                for k, v in mapping.items():
                    client.write_array(
                        v.read(),
                        key=k,
                        metadata=dict(v.metadata()),
                        specs=v.specs,
                    )
                yield client
    else:
        assert False


def test_key(client):
    "Binary operators with Key create query objects."
    assert (Key("a") == 1) == Eq("a", 1)
    assert (Key("a") != 2) == NotEq("a", 2)
    assert (Key("a") > 1) == Comparison("gt", "a", 1)
    assert (Key("a") < 1) == Comparison("lt", "a", 1)
    assert (Key("a") >= 1) == Comparison("ge", "a", 1)
    assert (Key("a") <= 1) == Comparison("le", "a", 1)


def test_eq(client):
    # Test encoding letters and ints.
    assert list(client.search(Eq("letter", "a"))) == ["a"]
    assert list(client.search(Eq("letter", "b"))) == ["b"]
    assert list(client.search(Eq("number", 0))) == ["a"]
    assert list(client.search(Eq("number", 1))) == ["b"]
    # Number is an int, not a string, so this should not match anything.
    assert list(client.search(Eq("number", "0"))) == []


def test_noteq(client):
    # Test encoding letters and ints.
    assert list(client.search(NotEq("letter", "a"))) != ["a"]
    assert list(client.search(NotEq("letter", "b"))) != ["b"]
    assert list(client.search(NotEq("number", 0))) != ["a"]
    assert list(client.search(NotEq("number", 1))) != ["b"]


def test_comparison(client):
    assert list(client.search(Comparison("gt", "number", 24))) == ["z"]
    assert list(client.search(Comparison("ge", "number", 24))) == ["y", "z"]
    assert list(client.search(Comparison("lt", "number", 1))) == ["a"]
    assert list(client.search(Comparison("le", "number", 1))) == ["a", "b"]


def test_contains(client):
    assert list(client.search(Contains("letters", "z"))) == ["does_contain_z"]


def test_full_text(client):
    "Basic test of FullText query"
    assert list(client.search(FullText("z"))) == ["z", "does_contain_z"]
    # plainto_tsquery fails to find certain words, weirdly, so it is a useful
    # test that we are using tsquery
    assert list(client.search(FullText("purple"))) == ["full_text_test_case"]
    assert list(client.search(FullText("urple"))) == ["full_text_test_case_urple"]


def test_full_text_after_migration():
    # Load a SQL database created by an older version of Tiled, predating FullText
    # support, and verify that the migration indexes the pre-existing metadata.
    with sqlite_from_dump("before_creating_fts5_virtual_table.sql") as database_path:
        subprocess.check_call(
            [sys.executable]
            + f"-m tiled catalog upgrade-database sqlite:///{database_path}".split()
        )
        catalog = from_uri(database_path)
        app = build_app(catalog)
        with Context.from_app(app) as context:
            client = from_context(context)
            assert list(client.search(FullText("blue"))) == ["x"]
            assert list(client.search(FullText("red"))) == []  # does not exist


def test_full_text_update(client):
    if client.metadata["backend"] == "map":
        pytest.skip("Updating not supported")
    # Update the fulltext index and check that it is current with the main data.
    try:
        client["full_text_test_case"].update_metadata({"color": "red"})
        assert list(client.search(FullText("purple"))) == []
        assert list(client.search(FullText("red"))) == ["full_text_test_case"]
    finally:
        # Reset case in the event tests are run out of order.
        client["full_text_test_case"].update_metadata({"color": "purple"})


def test_full_text_delete(client):
    if client.metadata["backend"] == "map":
        pytest.skip("Updating not supported")
    # Delete a record the fulltext index and check that it is current with the main data.
    client.write_array(numpy.ones(10), metadata={"item": "toaster"}, key="test_delete")
    # Assert that the data was written
    assert list(client.search(FullText("toaster"))) == ["test_delete"]
    client.delete_contents("test_delete", external_only=False)
    assert list(client.search(FullText("purple"))) == ["full_text_test_case"]
    assert list(client.search(FullText("toaster"))) == []


def test_regex(client):
    if client.metadata["backend"] in {"postgresql", "sqlite"}:

        def cm():
            return fail_with_status_code(HTTP_400_BAD_REQUEST)

    else:
        cm = nullcontext
    with cm():
        assert list(client.search(Regex("letter", "^z$"))) == ["z"]
    with cm():
        assert (
            list(client.search(Regex("letter", "^Z$"))) == []
        )  # default case_sensitive=True
    with cm():
        assert list(client.search(Regex("letter", "^Z$", case_sensitive=False))) == [
            "z"
        ]
    with cm():
        assert list(client.search(Regex("letter", "^Z$", case_sensitive=True))) == []
    with cm():
        assert list(client.search(Regex("letter", "[a-c]"))) == ["a", "b", "c"]
    with cm():
        # Check that if the key is not a string it is ignored.
        assert list(client.search(Regex("letters", "anything"))) == []


def test_not_and_and_or(client):
    with pytest.raises(TypeError):
        not (Key("color") == "red")  # type: ignore
    with pytest.raises(TypeError):
        (Key("color") == "red") and (Key("sample") == "Ni")  # type: ignore
    with pytest.raises(TypeError):
        (Key("color") == "red") or (Key("sample") == "Ni")  # type: ignore


@pytest.mark.parametrize(
    "query_values",
    [
        ["a", "k", "z"],
        ("a", "k", "z"),
        {"a", "k", "z"},
        {"a", "k", "z", "a", "z", "z"},
    ],
)
def test_in(client, query_values):
    assert sorted(list(client.search(In("letter", query_values)))) == [
        "a",
        "k",
        "z",
    ]


def test_in_empty(client):
    assert list(client.search(In("letter", []))) == []


@pytest.mark.parametrize(
    "query_values",
    [
        ["a", "k", "z"],
        ("a", "k", "z"),
        {"a", "k", "z"},
        {"a", "k", "z", "a", "z", "z"},
    ],
)
def test_notin(client, query_values):
    # TODO: Postgres and SQlite ACTUALLY treat this query differently in external testing.
    # SQLite WILL NOT include fields that do not have the key, which is correct.
    # Postgres WILL include fields that do not have the key,
    # because by extension they do not have the value. Also correct. Why?
    assert sorted(list(client.search(NotIn("letter", query_values)))) == sorted(
        list(
            set(
                list(mapping.keys())
                if client.metadata["backend"] == "postgresql"
                else keys
            )
            - set(["a", "k", "z"])
        )
    )


def test_not_in_empty(client):
    assert sorted(list(client.search(NotIn("letter", [])))) == sorted(list(client))


@pytest.mark.parametrize(
    "include_values,exclude_values",
    [
        (["foo", "bar"], ["baz"]),
        (("foo", "bar"), ("baz",)),
        ({"foo", "bar"}, {"baz"}),
        ({"foo", "bar", "foo", "bar", "bar"}, {"baz", "baz", "baz"}),
    ],
)
def test_specs(client, include_values, exclude_values):
    assert list(
        client.search(SpecsQuery(include=include_values, exclude=exclude_values))
    ) == ["specs_foo_bar"]


def test_structure_families(client):
    with pytest.raises(ValueError):
        StructureFamilyQuery("foo")

    assert set(client.search(StructureFamilyQuery("array"))) == set(mapping)


def test_key_present(client):
    if client.metadata["backend"] == "map":
        pytest.skip("No 'KeyPresent' support on MapAdapter")
    # These containers have a "color" key.
    assert list(client.search(KeyPresent("color"))) == [
        "full_text_test_case",
        "full_text_test_case_urple",
    ]
    # outer key present
    assert list(client.search(KeyPresent("nested"))) == ["nested_key_test"]
    # one of the inner keys present
    assert list(client.search(KeyPresent("nested.nested-key-1"))) == ["nested_key_test"]
    assert list(client.search(KeyPresent("nested.nested-key-2"))) == ["nested_key_test"]
    # both inner keys
    assert list(
        client.search(KeyPresent("nested.nested-key-1")).search(
            KeyPresent("nested.nested-key-2")
        )
    ) == ["nested_key_test"]
    # inner key not present
    assert list(client.search(KeyPresent("nested.nested-key-3"))) == []
    # outer key not present
    assert list(client.search(KeyPresent("nonsense.nested-key-1"))) == []
    # These are all the containers that do not have a "number" key.
    assert list(client.search(KeyPresent("number", False))) == [
        "does_contain_z",
        "does_not_contain_z",
        "full_text_test_case",
        "full_text_test_case_urple",
        "specs_foo_bar",
        "specs_foo_bar_baz",
        "nested_key_test",
    ]


def test_keys_filter(client):
    expected = ["a", "b", "c"]
    results = client.search(KeysFilter(keys=expected))
    assert set(results) == set(expected)


def test_like(client):
    if client.metadata["backend"] == "map":
        pytest.skip("No 'LIKE' support on MapAdapter")
    expected = ["full_text_test_case", "full_text_test_case_urple"]
    results = client.search(Like("color", "%urple"))
    assert set(results) == set(expected)
