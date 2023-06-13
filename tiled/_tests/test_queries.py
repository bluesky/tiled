import string

import numpy
import pytest

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
    KeysFilter,
    NotEq,
    NotIn,
    Regex,
    Specs,
    StructureFamily,
)
from ..server.app import build_app
from .conftest import TILED_TEST_POSTGRESQL_URI
from .utils import temp_postgres_sync

keys = list(string.ascii_lowercase)
mapping = {
    letter: ArrayAdapter.from_array(
        number * numpy.ones(10), metadata={"letter": letter, "number": number}
    )
    for letter, number in zip(keys, range(26))
}
mapping["does_contain_z"] = ArrayAdapter.from_array(
    numpy.ones(10), metadata={"letters": list(string.ascii_lowercase)}
)
mapping["does_not_contain_z"] = ArrayAdapter.from_array(
    numpy.ones(10), metadata={"letters": list(string.ascii_lowercase[:-1])}
)

mapping["specs_foo_bar"] = ArrayAdapter.from_array(numpy.ones(10), specs=["foo", "bar"])
mapping["specs_foo_bar_baz"] = ArrayAdapter.from_array(
    numpy.ones(10), specs=["foo", "bar", "baz"]
)


@pytest.fixture(scope="module", params=["map", "sqlite", "postgresql"])
def client(request, tmpdir_module):
    if request.param == "map":
        tree = MapAdapter(mapping)
    elif request.param == "sqlite":
        tree = in_memory(writable_storage=tmpdir_module)
    elif request.param == "postgresql":
        if not TILED_TEST_POSTGRESQL_URI:
            raise pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
        # Create temporary database.
        with temp_postgres_sync(TILED_TEST_POSTGRESQL_URI) as uri_with_database_name:
            # Build an adapter on it.
            adapter = from_uri(
                uri_with_database_name,
                writable_storage=str(tmpdir_module),
                initialize_database_at_startup=True,
            )
            yield adapter
    else:
        assert False
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        if request.param == "catalog":
            for k, v in mapping.items():
                client.write_array(v.read(), key=k, metadata=dict(v.metadata))
        yield client


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
    pytest.xfail("Not yet implemented on SQL-backed catalog")
    assert list(client.search(FullText("z"))) == ["z", "does_contain_z"]


def test_regex(client):
    pytest.xfail("Not yet implemented on SQL-backed catalog")
    assert list(client.search(Regex("letter", "^z$"))) == ["z"]
    assert (
        list(client.search(Regex("letter", "^Z$"))) == []
    )  # default case_sensitive=True
    assert list(client.search(Regex("letter", "^Z$", case_sensitive=False))) == ["z"]
    assert list(client.search(Regex("letter", "^Z$", case_sensitive=True))) == []
    assert list(client.search(Regex("letter", "[a-c]"))) == ["a", "b", "c"]
    # Check that if the key is not a string it is ignored.
    assert list(client.search(Regex("letters", "anything"))) == []


def test_not_and_and_or(client):
    with pytest.raises(TypeError):
        not (Key("color") == "red")
    with pytest.raises(TypeError):
        (Key("color") == "red") and (Key("sample") == "Ni")
    with pytest.raises(TypeError):
        (Key("color") == "red") or (Key("sample") == "Ni")


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
    assert sorted(list(client.search(In("letter", query_values)))) == ["a", "k", "z"]


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
    assert sorted(list(client.search(NotIn("letter", query_values)))) == sorted(
        list(set(keys) - set(["a", "k", "z"]))
    )


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
    pytest.xfail("Not yet implemented on SQL-backed catalog")
    with pytest.raises(TypeError):
        Specs("foo")

    assert sorted(list(client.search(Specs(include=include_values)))) == sorted(
        ["specs_foo_bar", "specs_foo_bar_baz"]
    )

    assert list(
        client.search(Specs(include=include_values, exclude=exclude_values))
    ) == ["specs_foo_bar"]


def test_structure_families(client):
    with pytest.raises(ValueError):
        StructureFamily("foo")

    assert set(client.search(StructureFamily("array"))) == set(mapping)


def test_keys_filter(client):
    expected = ["a", "b", "c"]
    results = client.search(KeysFilter(keys=expected))
    assert set(results) == set(expected)
