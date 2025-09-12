from pathlib import Path

import pytest

from ..utils import (
    CachingMap,
    DictView,
    ListView,
    OneShotCachedMap,
    ensure_specified_sql_driver,
    parse_mimetype,
    parse_time_string,
    sanitize_uri,
    walk,
)


def test_ensure_specified_sql_driver():
    # Postgres
    # Default driver is added if missing.
    assert (
        ensure_specified_sql_driver(
            "postgresql://user:password@localhost:5432/database"
        )
        == "postgresql+asyncpg://user:password@localhost:5432/database"
    )
    # Default driver passes through if specified.
    assert (
        ensure_specified_sql_driver(
            "postgresql+asyncpg://user:password@localhost:5432/database"
        )
        == "postgresql+asyncpg://user:password@localhost:5432/database"
    )
    # Do not override user-provided.
    assert (
        ensure_specified_sql_driver(
            "postgresql+custom://user:password@localhost:5432/database"
        )
        == "postgresql+custom://user:password@localhost:5432/database"
    )

    # SQLite
    # Default driver is added if missing.
    assert (
        ensure_specified_sql_driver("sqlite:////test.db")
        == "sqlite+aiosqlite:////test.db"
    )
    # Default driver passes through if specified.
    assert (
        ensure_specified_sql_driver("sqlite+aiosqlite:////test.db")
        == "sqlite+aiosqlite:////test.db"
    )
    # Do not override user-provided.
    assert (
        ensure_specified_sql_driver("sqlite+custom:////test.db")
        == "sqlite+custom:////test.db"
    )
    # Handle SQLite :memory: URIs
    assert (
        ensure_specified_sql_driver("sqlite+aiosqlite://:memory:")
        == "sqlite+aiosqlite://:memory:"
    )
    assert (
        ensure_specified_sql_driver("sqlite://:memory:")
        == "sqlite+aiosqlite://:memory:"
    )
    # Handle SQLite relative URIs
    assert (
        ensure_specified_sql_driver("sqlite+aiosqlite:///test.db")
        == "sqlite+aiosqlite:///test.db"
    )
    assert (
        ensure_specified_sql_driver("sqlite:///test.db")
        == "sqlite+aiosqlite:///test.db"
    )
    # Filepaths are implicitly SQLite databases.
    # Relative path
    assert ensure_specified_sql_driver("test.db") == "sqlite+aiosqlite:///test.db"
    # Path object
    assert ensure_specified_sql_driver(Path("test.db")) == "sqlite+aiosqlite:///test.db"
    # Relative path anchored to .
    assert ensure_specified_sql_driver("./test.db") == "sqlite+aiosqlite:///test.db"
    # Absolute path
    assert (
        ensure_specified_sql_driver(Path("/tmp/test.db"))
        == f"sqlite+aiosqlite:///{Path('/tmp/test.db')}"
    )


@pytest.mark.parametrize(
    "string_input,expected",
    [
        ("3s", 3),
        ("7m", 7 * 60),
        ("5h", 5 * 60 * 60),
        ("1d", 1 * 24 * 60 * 60),
        ("2y", 2 * 365 * 24 * 60 * 60),
    ],
)
def test_parse_time_string_valid(string_input, expected):
    assert parse_time_string(string_input) == expected


@pytest.mark.parametrize(
    "string_input",
    [
        "3z",  # unrecognized units
        "3M",  # unrecognized units
        "-3m",  # invalid character '-'
        "3 m",  # invalid character '-'
    ],
)
def test_parse_time_string_invalid(string_input):
    with pytest.raises(ValueError):
        parse_time_string(string_input)


def test_listview_repr():
    lv = ListView([1, 2, 3])
    assert repr(lv) == "ListView([1, 2, 3])"


def test_dictview_repr():
    dv = DictView({"a": 1, "b": 2})
    assert repr(dv) == "DictView({'a': 1, 'b': 2})"


def test_listview_repr_pretty(monkeypatch):
    lv = ListView([1, 2, 3])
    called = {}

    class DummyP:
        "Dummy pretty printer to capture the text output."

        def text(self, s):
            called["text"] = s

    # Should use pformat on a list
    lv._repr_pretty_(DummyP(), cycle=False)
    assert called["text"] == "[1, 2, 3]"

    # Should convert to list if not a list
    lv2 = ListView((4, 5, 6))
    lv2._internal_list = (4, 5, 6)  # forcibly set to tuple
    called.clear()
    lv2._repr_pretty_(DummyP(), cycle=False)
    assert called["text"] == "[4, 5, 6]"


def test_dictview_repr_pretty(monkeypatch):
    dv = DictView({"a": 1, "b": 2})
    called = {}

    class DummyP:
        "Dummy pretty printer to capture the text output."

        def text(self, s):
            called["text"] = s

    # Should use pformat on a dict
    dv._repr_pretty_(DummyP(), cycle=False)
    assert called["text"] == "{'a': 1, 'b': 2}"

    # Should convert to dict if not a dict
    dv2 = DictView([("x", 10), ("y", 20)])
    dv2._internal_dict = [("x", 10), ("y", 20)]  # forcibly set to list of tuples
    called.clear()
    dv2._repr_pretty_(DummyP(), cycle=False)
    # The order of keys in dict may not be guaranteed, so check both possibilities
    assert called["text"] in ("{'x': 10, 'y': 20}", "{'y': 20, 'x': 10}")


def test_oneshotcachedmap_repr_lazy_and_evaluated():
    # Value factories
    def factory1() -> int:
        return 42

    def factory2() -> str:
        return "foo"

    # All values are lazy initially
    m = OneShotCachedMap(a=factory1, b=factory2)
    r = repr(m)
    assert "<OneShotCachedMap" in r
    assert "'a': <lazy>" in r
    assert "'b': <lazy>" in r

    # Access one value to trigger evaluation
    assert m["a"] == 42
    r2 = repr(m)
    assert "'a': 42" in r2
    assert "'b': <lazy>" in r2

    # Access both
    assert m["b"] == "foo"
    r3 = repr(m)
    assert "'a': 42" in r3
    assert "'b': 'foo'" in r3


def test_cachingmap_repr_lazy_and_evaluated():
    # Value factories
    def factory1() -> int:
        return 123

    def factory2() -> str:
        return "bar"

    mapping = {"x": factory1, "y": factory2}
    cache = {}

    m = CachingMap(mapping.copy(), cache)
    # Initially, nothing is cached, so repr should show <lazy>
    r = repr(m)
    assert "<CachingMap" in r
    assert "'x': <lazy>" in r
    assert "'y': <lazy>" in r

    # Access one value to trigger evaluation and caching
    assert m["x"] == 123
    r2 = repr(m)
    assert "'x': 123" in r2
    assert "'y': <lazy>" in r2

    # Access both
    assert m["y"] == "bar"
    r3 = repr(m)
    assert "'x': 123" in r3
    assert "'y': 'bar'" in r3

    # If cache is None, all should be <lazy>
    m2 = CachingMap(mapping.copy(), None)
    r4 = repr(m2)
    assert "'x': <lazy>" in r4
    assert "'y': <lazy>" in r4


@pytest.mark.parametrize(
    "mapping, expected",
    [
        (
            {
                "A": {
                    "dog": {},
                    "cat": {},
                    "monkey": {},
                },
                "B": {
                    "snake": {},
                    "bear": {},
                    "wolf": {},
                },
            },
            [
                ["A"],
                ["A", "dog"],
                ["A", "cat"],
                ["A", "monkey"],
                ["B"],
                ["B", "snake"],
                ["B", "bear"],
                ["B", "wolf"],
            ],
        ),
        ({"root": 42}, [["root"]]),
        ({"x": {"y": {"z": 1}}, "a": 2}, [["x"], ["x", "y"], ["x", "y", "z"], ["a"]]),
        ({}, []),
        ({"foo": object()}, [["foo"]]),
    ],
    ids=["nested_dict", "leaf_value", "nested_mixed", "empty_dict", "non_dict_leaf"],
)
def test_walk(mapping, expected):
    result = list(walk(mapping))
    assert result == expected


@pytest.mark.parametrize(
    "uri, expected_clean_uri, expected_username, expected_password",
    [
        # URI with username and password
        (
            "postgresql://user:pass@localhost:5432/db",
            "postgresql://localhost:5432/db",
            "user",
            "pass",
        ),
        # URI with only username
        (
            "postgresql://user@localhost:5432/db",
            "postgresql://localhost:5432/db",
            "user",
            None,
        ),
        # URI with no username/password
        (
            "postgresql://localhost:5432/db",
            "postgresql://localhost:5432/db",
            None,
            None,
        ),
        # URI with username and password, no port
        (
            "sqlite://user:pass@localhost/db",
            "sqlite://localhost/db",
            "user",
            "pass",
        ),
        # URI with username only, no port
        (
            "sqlite://user@localhost/db",
            "sqlite://localhost/db",
            "user",
            None,
        ),
        # URI with username and password, with query and fragment
        (
            "postgresql://user:pass@localhost:5432/db?foo=bar#frag",
            "postgresql://localhost:5432/db?foo=bar#frag",
            "user",
            "pass",
        ),
        # URI with username only, with query and fragment
        (
            "postgresql://user@localhost:5432/db?foo=bar#frag",
            "postgresql://localhost:5432/db?foo=bar#frag",
            "user",
            None,
        ),
        # URI with no netloc (should not fail)
        (
            "sqlite:///db.sqlite",
            "sqlite:///db.sqlite",
            None,
            None,
        ),
    ],
)
def test_sanitize_uri(uri, expected_clean_uri, expected_username, expected_password):
    clean_uri, username, password = sanitize_uri(uri)
    assert clean_uri == expected_clean_uri
    assert username == expected_username
    assert password == expected_password


@pytest.mark.parametrize(
    "mimetype, expected",
    [
        ("text/csv", ("text/csv", {})),
        ("text/csv;header=absent", ("text/csv", {"header": "absent"})),
        (
            "text/csv;header=absent; charset=utf-8",
            ("text/csv", {"header": "absent", "charset": "utf-8"}),
        ),
        (
            "text/csv; header=absent; charset=utf-8",
            ("text/csv", {"header": "absent", "charset": "utf-8"}),
        ),
    ],
)
def test_parse_valid_mimetype(mimetype, expected):
    assert parse_mimetype(mimetype) == expected


def test_parse_invalid_mimetype():
    with pytest.raises(ValueError):
        # Parameter does not have form 'key=value'
        assert parse_mimetype("text/csv;oops")
