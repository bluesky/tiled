import asyncio
import string
import subprocess
import sys
from contextlib import nullcontext

import numpy
import pytest
import pytest_asyncio
from starlette.status import HTTP_400_BAD_REQUEST

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.catalog import from_uri, in_memory
from tiled.client import Context, from_context
from tiled.queries import (
    Comparison,
    Contains,
    Eq,
    FullText,
    In,
    Key,
    KeyLike,
    KeyPresent,
    KeysFilter,
    Like,
    NotEq,
    NotIn,
    QueryValueError,
    Regex,
    SpecsQuery,
    StructureFamilyQuery,
)
from tiled.server.app import build_app

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
            tree = from_uri(
                uri_with_database_name,
                writable_storage=str(tmpdir_module / "postgresql"),
                metadata={"backend": request.param},
                init_if_not_exists=True,
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


def test_eq_none_sqlite(tmpdir):
    "On SQLite, a JSON null in the metadata is not confused with a missing key."
    tree = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        client.create_container("null", metadata={"x": None})
        client.create_container("one", metadata={"x": 1})
        client.create_container("missing", metadata={"y": 2})
        assert sorted(client.search(Eq("x", None))) == ["null"]
        assert sorted(client.search(NotEq("x", None))) == ["one"]


def test_in_none_sqlite(tmpdir):
    "On SQLite, None in an In/NotIn list matches a JSON null, not a missing key."
    tree = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        client.create_container("null", metadata={"x": None})
        client.create_container("one", metadata={"x": 1})
        client.create_container("two", metadata={"x": 2})
        client.create_container("missing", metadata={"y": 2})
        assert sorted(client.search(In("x", [None]))) == ["null"]
        assert sorted(client.search(In("x", [None, 1]))) == ["null", "one"]
        assert sorted(client.search(NotIn("x", [None]))) == ["one", "two"]
        assert sorted(client.search(NotIn("x", [None, 1]))) == ["two"]


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


def _search_http(client, params):
    return client.context.http_client.get(client.item["links"]["search"], params=params)


def test_same_query_type_given_twice(client):
    "Repeated filter parameters of one query type are paired up by position."
    assert list(client.search(Eq("letter", "a")).search(Eq("number", 0))) == ["a"]
    assert list(client.search(Eq("letter", "a")).search(Eq("number", 1))) == []
    response = _search_http(
        client,
        {
            "filter[eq][condition][key]": ["letter", "number"],
            "filter[eq][condition][value]": ['"a"', "1"],
        },
    )
    response.raise_for_status()
    assert response.json()["data"] == []


def test_filter_parameters_given_different_numbers_of_times(client):
    "Values that cannot be paired up are rejected, not dropped or misapplied."
    response = _search_http(
        client,
        {
            "filter[eq][condition][key]": ["letter", "number"],
            "filter[eq][condition][value]": ['"a"'],
        },
    )
    assert response.status_code == HTTP_400_BAD_REQUEST
    detail = response.json()["detail"]
    assert "'eq'" in detail
    assert "different numbers of times" in detail


def test_filter_parameters_missing_required(client):
    response = _search_http(client, {"filter[eq][condition][key]": "letter"})
    assert response.status_code == HTTP_400_BAD_REQUEST
    assert (
        "missing the required filter parameter(s) 'value'"
        in (response.json()["detail"])
    )


def test_filter_parameters_optional_may_be_omitted(client):
    "Parameters with a default may be left out, as long as consistently."
    response = _search_http(
        client,
        {
            "filter[regex][condition][key]": "letter",
            "filter[regex][condition][pattern]": "^z$",
        },
    )
    if client.metadata["backend"] == "map":
        response.raise_for_status()
        assert [item["id"] for item in response.json()["data"]] == ["z"]
    else:
        # Regex is not supported on SQL catalogs; the request is still parsed.
        assert response.status_code == HTTP_400_BAD_REQUEST
        assert "not supported" in response.json()["detail"]


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


# KeyLike gets its own catalogs, so that these keys do not change the results
# of the other tests, which share the module-scoped `client` fixture.
KEY_LIKE_KEYS = [
    "scan1",
    "Scan2",
    "prescan",
    "scanner",
    "no_scan_here",
    "a%b",
    "axb",
    "a_b",
    "ab",
    "a\\b",
    "a*b",
]


@pytest.fixture
def key_like_client(catalog_adapter):
    with Context.from_app(build_app(catalog_adapter)) as context:
        client = from_context(context)
        for key in KEY_LIKE_KEYS:
            client.create_container(key)
        yield client


@pytest.mark.parametrize(
    "query, expected",
    [
        # Contains (the default), starts with, ends with
        (KeyLike("scan"), {"scan1", "prescan", "scanner", "no_scan_here"}),
        (KeyLike("scan", before=False), {"scan1", "scanner"}),
        (KeyLike("scan", after=False), {"prescan"}),
        # Case-sensitive
        (KeyLike("Scan"), {"Scan2"}),
        (KeyLike("SCAN"), set()),
        # Text longer than the keys it would otherwise match
        (KeyLike("xprescan", after=False), set()),
        (KeyLike("scan1x", before=False), set()),
        # No character is a wildcard.
        (KeyLike("%"), {"a%b"}),
        (KeyLike("_"), {"a_b", "no_scan_here"}),
        (KeyLike("*"), {"a*b"}),
        (KeyLike("\\"), {"a\\b"}),
        (KeyLike("a%", before=False), {"a%b"}),
        (KeyLike("_b", after=False), {"a_b"}),
        (KeyLike("a_b"), {"a_b"}),
    ],
)
def test_key_like(key_like_client, query, expected):
    assert set(key_like_client.search(query)) == expected


def test_key_like_composes_with_other_queries(key_like_client):
    results = key_like_client.search(KeyLike("scan")).search(
        KeyLike("scan", after=False)
    )
    assert set(results) == {"prescan"}
    results = key_like_client.search(KeyLike("scan")).search(KeyLike("_"))
    assert set(results) == {"no_scan_here"}


def test_key_like_lookup_within_results(key_like_client):
    results = key_like_client.search(KeyLike("scan"))
    assert "prescan" in results
    assert "Scan2" not in results
    results["prescan"]
    with pytest.raises(KeyError):
        results["Scan2"]


def test_key_like_deep(key_like_client):
    key_like_client["scan1"].create_container("inner_scan")
    key_like_client["axb"].create_container("Inner_SCAN")
    results = key_like_client.search_deep(KeyLike("scan"))
    assert set(results) == {
        ("scan1",),
        ("prescan",),
        ("scanner",),
        ("no_scan_here",),
        ("scan1", "inner_scan"),
    }


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"key": "scan", "before": False, "after": False}, "at least one of"),
        ({"key": ""}, "non-empty"),
    ],
)
def test_key_like_invalid_is_rejected_by_constructor(kwargs, message):
    "The Python query object rejects invalid parameters before any request."
    with pytest.raises(QueryValueError, match=message) as info:
        KeyLike(**kwargs)
    detail = str(info.value)
    assert detail.startswith("KeyLike ")
    if "at least one of" in message:
        # Python users are pointed at the Python classes.
        assert "KeyLookup or KeysFilter" in detail
        assert "keys_filter" not in detail


@pytest.mark.parametrize(
    "params, message",
    [
        (
            {
                "filter[key_like][condition][key]": "scan",
                "filter[key_like][condition][before]": "false",
                "filter[key_like][condition][after]": "false",
            },
            "at least one of",
        ),
        ({"filter[key_like][condition][key]": ""}, "non-empty"),
    ],
)
def test_key_like_invalid_is_rejected_by_server(key_like_client, params, message):
    "Clients that do not use the Python query objects get an HTTP 400."
    response = key_like_client.context.http_client.get(
        key_like_client.item["links"]["search"], params=params
    )
    assert response.status_code == HTTP_400_BAD_REQUEST
    detail = response.json()["detail"]
    assert message in detail
    assert detail.startswith("The key_like query ")
    if "at least one of" in message:
        # Other clients are pointed at the HTTP query types, not Python classes.
        assert "the lookup or keys_filter query" in detail
        assert "KeysFilter" not in detail


def test_key_like_http_defaults(key_like_client):
    "Over HTTP, 'before' and 'after' default to true and accept boolean strings."
    link = key_like_client.item["links"]["search"]
    http_client = key_like_client.context.http_client

    def keys(params):
        response = http_client.get(link, params=params)
        response.raise_for_status()
        return {item["id"] for item in response.json()["data"]}

    assert keys({"filter[key_like][condition][key]": "scan"}) == {
        "scan1",
        "prescan",
        "scanner",
        "no_scan_here",
    }
    assert keys(
        {
            "filter[key_like][condition][key]": "scan",
            "filter[key_like][condition][before]": "false",
            "filter[key_like][condition][after]": "true",
        }
    ) == {"scan1", "scanner"}


def test_key_like_unsupported_on_map_adapter():
    tree = MapAdapter({"scan1": ArrayAdapter.from_array(numpy.ones(3))})
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        with fail_with_status_code(HTTP_400_BAD_REQUEST):
            list(client.search(KeyLike("scan")))
