import string

import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_tree
from ..queries import (
    Comparison,
    Contains,
    Eq,
    FullText,
    In,
    Key,
    NotEq,
    NotIn,
    Regex,
    Specs,
    StructureFamily,
)

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

mapping["specs_foo_bar"] = MapAdapter({}, specs=["foo", "bar"])
mapping["specs_foo_bar_baz"] = MapAdapter({}, specs=["foo", "bar", "baz"])
tree = MapAdapter(mapping)
client = from_tree(tree)


def test_key():
    "Binary operators with Key create query objects."
    assert (Key("a") == 1) == Eq("a", 1)
    assert (Key("a") != 2) == NotEq("a", 2)
    assert (Key("a") > 1) == Comparison("gt", "a", 1)
    assert (Key("a") < 1) == Comparison("lt", "a", 1)
    assert (Key("a") >= 1) == Comparison("ge", "a", 1)
    assert (Key("a") <= 1) == Comparison("le", "a", 1)


def test_eq():
    # Test encoding letters and ints.
    assert list(client.search(Eq("letter", "a"))) == ["a"]
    assert list(client.search(Eq("letter", "b"))) == ["b"]
    assert list(client.search(Eq("number", 0))) == ["a"]
    assert list(client.search(Eq("number", 1))) == ["b"]
    # Number is an int, not a string, so this should not match anything.
    assert list(client.search(Eq("number", "0"))) == []


def test_noteq():
    # Test encoding letters and ints.
    assert list(client.search(NotEq("letter", "a"))) != ["a"]
    assert list(client.search(NotEq("letter", "b"))) != ["b"]
    assert list(client.search(NotEq("number", 0))) != ["a"]
    assert list(client.search(NotEq("number", 1))) != ["b"]


def test_comparison():
    assert list(client.search(Comparison("gt", "number", 24))) == ["z"]
    assert list(client.search(Comparison("ge", "number", 24))) == ["y", "z"]
    assert list(client.search(Comparison("lt", "number", 1))) == ["a"]
    assert list(client.search(Comparison("le", "number", 1))) == ["a", "b"]


def test_contains():
    assert list(client.search(Contains("letters", "z"))) == ["does_contain_z"]


def test_full_text():
    assert list(client.search(FullText("z"))) == ["z", "does_contain_z"]


def test_regex():
    assert list(client.search(Regex("letter", "^z$"))) == ["z"]
    assert (
        list(client.search(Regex("letter", "^Z$"))) == []
    )  # default case_sensitive=True
    assert list(client.search(Regex("letter", "^Z$", case_sensitive=False))) == ["z"]
    assert list(client.search(Regex("letter", "^Z$", case_sensitive=True))) == []
    assert list(client.search(Regex("letter", "[a-c]"))) == ["a", "b", "c"]
    # Check that if the key is not a string it is ignored.
    assert list(client.search(Regex("letters", "anything"))) == []


def test_not_and_and_or():
    with pytest.raises(TypeError):
        not (Key("color") == "red")
    with pytest.raises(TypeError):
        (Key("color") == "red") and (Key("sample") == "Ni")
    with pytest.raises(TypeError):
        (Key("color") == "red") or (Key("sample") == "Ni")


def test_in():
    assert list(client.search(In("letter", ["a", "k", "z"]))) == ["a", "k", "z"]


def test_notin():
    assert list(client.search(NotIn("letter", ["a", "k", "z"]))) == sorted(
        list(set(keys) - set(["a", "k", "z"]))
    )


def test_specs():
    with pytest.raises(TypeError):
        Specs("foo")

    assert list(client.search(Specs(include=["foo", "bar"]))) == sorted(
        ["specs_foo_bar", "specs_foo_bar_baz"]
    )

    assert list(client.search(Specs(include=["foo", "bar"], exclude=["baz"]))) == [
        "specs_foo_bar"
    ]


def test_structure_families():
    with pytest.raises(ValueError):
        StructureFamily("foo")

    assert list(client.search(StructureFamily("node"))) == sorted(
        ["specs_foo_bar", "specs_foo_bar_baz"]
    )
