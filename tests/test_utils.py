import math
from pathlib import Path

import dask.array as da
import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tiled.client.utils import slices_to_dask_chunks, split_1d, split_nd_slice
from tiled.ndslice import NDSlice
from tiled.utils import (
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


@given(data=st.data(), max_len=st.integers(1, 50))
def test_split_1d(data, max_len):
    # Generate start and stop values
    start = data.draw(st.integers(0, 100), label="start")
    stop = data.draw(st.integers(0, 100), label="stop")

    # Check degenerate slice
    if start == stop:
        result = split_1d(start, stop, step=1, max_len=max_len, pref_splits=[])
        assert result == [(start, stop)]
        return

    # Step must match slice direction
    if stop > start:
        step = data.draw(st.integers(1, 10), label="step")
    elif stop < start:
        step = data.draw(st.integers(-10, -1), label="step")

    # Preferred splits strictly inside slice and unique
    preferred_splits = data.draw(
        st.lists(
            st.integers(min(start, stop), max(start, stop) - 1),
            unique=True,
            max_size=10,
        ),
        label="preferred_splits",
    )

    result = split_1d(start, stop, step, max_len, preferred_splits)

    # 1. Check that first and last boundaries match
    assert result[0][0] == start
    assert result[-1][1] == stop

    # 2. Check contiguous intervals
    for (_, a_stop), (b_start, _) in zip(result, result[1:]):
        assert a_stop == b_start

    # 3. Check grid alignment
    for a, b in result:
        assert (a - start) % step == 0
        if b != stop:
            assert (b - start) % step == 0

    # 4. Check max length constraint
    for a, b in result:
        assert len(range(a, b, step)) <= max_len

    # 5. Check step direction consistency
    for a, b in result:
        if step > 0:
            assert b >= a
        else:
            assert b <= a

    # 6. Check no degenerate intervals
    for a, b in result:
        assert a != b
        assert len(range(a, b, step)) > 0


@st.composite
def nd_slice_strategy(draw):
    ndim = draw(st.integers(1, 4))

    starts = draw(st.lists(st.integers(0, 20), min_size=ndim, max_size=ndim))
    lengths = draw(st.lists(st.integers(0, 20), min_size=ndim, max_size=ndim))
    steps = draw(st.lists(st.integers(1, 5), min_size=ndim, max_size=ndim))
    reversed = draw(st.lists(st.booleans(), min_size=ndim, max_size=ndim))

    slices, shape = [], []

    for start, length, step, rev in zip(starts, lengths, steps, reversed):
        if length == 0:
            # Singleton dimension, use an integer index
            slices.append(start)
            shape.append(start + 1)
        else:
            stop = start + length * step
            if rev:
                slices.append(slice(stop, start, -step))
            else:
                slices.append(slice(start, stop, step))
            shape.append(stop)

    shape = tuple(shape)

    return NDSlice(*slices).expand_for_shape(shape), shape


@st.composite
def pref_split_strategy(draw, slices):
    pref = []

    for sl in slices:
        if isinstance(sl, int):
            pref.append([])  # No splits for singleton dimensions
            continue

        start, stop, step = sl.start, sl.stop, sl.step or 1
        grid = list(range(start + step, stop, step))

        if grid:
            splits = draw(
                st.lists(
                    st.sampled_from(grid),
                    unique=True,
                    max_size=min(len(grid), 5),
                )
            )
        else:
            splits = []

        pref.append(sorted(splits))

    return pref


@given(data=st.data(), max_size=st.sampled_from([1, 2, 3, 5, 50, 100, 200]))
@settings(deadline=None)
def test_split_nd_slice(data, max_size):
    arr_slice, shape = data.draw(nd_slice_strategy(), label="nd_slice")

    if math.prod(arr_slice.shape_after_slice(shape)) == 0:
        # Skip degenerate case where slice results in empty array
        return

    pref_splits = data.draw(pref_split_strategy(arr_slice), label="pref_splits")
    arr = np.arange(math.prod(shape)).reshape(shape)
    slices = split_nd_slice(arr_slice, max_size, pref_splits)

    # 1. Check reconstruction of the original slice from the pieces
    darr = da.Array(
        name="test",
        dask={
            ("test",) + indx: (lambda x: arr[x], slc) for indx, slc in slices.items()
        },
        dtype=arr.dtype,
        chunks=slices_to_dask_chunks(slices, shape),
        shape=arr_slice.shape_after_slice(shape),
    )
    np.testing.assert_array_equal(darr.compute(), arr[arr_slice])

    # 2. Check that each slice respects the max_size constraint
    for slc in slices.values():
        slc_shape = slc.shape_after_slice(shape)
        assert math.prod(slc_shape) <= max_size


@pytest.mark.parametrize(
    "slice_dict,shape,expected",
    [
        ({(0, 0): NDSlice.from_numpy_str(":10, :5")}, (10, 5), ((10,), (5,))),
        (
            {
                (0,): NDSlice.from_numpy_str(":10"),
                (1,): NDSlice.from_numpy_str("10:30"),
            },
            (30,),
            ((10, 20),),
        ),
        (
            {
                (0, 0): NDSlice.from_numpy_str(":10, :5"),
                (0, 1): NDSlice.from_numpy_str(":10, 5:12"),
                (1, 0): NDSlice.from_numpy_str("10:18, :5"),
                (1, 1): NDSlice.from_numpy_str("10:18, 5:12"),
            },
            (18, 12),
            ((10, 8), (5, 7)),
        ),
        (
            {
                (0, 0, 0): NDSlice.from_numpy_str(":2, :3, :4"),
                (0, 0, 1): NDSlice.from_numpy_str(":2, :3, 4:9"),
                (1, 0, 0): NDSlice.from_numpy_str("2:8, :3, :4"),
                (1, 0, 1): NDSlice.from_numpy_str("2:8, :3, 4:9"),
            },
            (8, 3, 9),
            ((2, 6), (3,), (4, 5)),
        ),
    ],
)
def test_dask_slices(slice_dict, shape, expected):
    assert slices_to_dask_chunks(slice_dict, shape) == expected
