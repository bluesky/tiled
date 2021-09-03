import pytest

from ..utils import OneShotCachedMap, CachingMap


def test_one_shot_cached_map():
    OneShotCachedMap({})  # empty is allowed

    counter = 0

    def f():
        nonlocal counter
        counter += 1
        return 5

    d = OneShotCachedMap({"a": f})
    # These operations do not cause f to be called.
    len(d)
    "a" in d
    list(d)
    assert counter == 0

    assert d["a"] == 5  # f is called here
    assert counter == 1
    assert d["a"] == 5  # but not again
    assert counter == 1

    with pytest.raises(TypeError):
        del d["a"]
    with pytest.raises(TypeError):
        d["a"] = 5


def test_caching_map():
    CachingMap({}, {})  # empty is allowed

    counter = 0

    def f():
        nonlocal counter
        counter += 1
        return 5

    cache = {}
    mapping = {"a": f}
    d = CachingMap(mapping, cache)

    # These operations do not cause f to be called.
    len(d)
    "a" in d
    list(d)
    assert counter == 0

    assert d["a"] == 5  # f is called here
    assert counter == 1
    assert d["a"] == 5  # but not again
    assert counter == 1
    cache.clear()  # underlying cache evicts
    assert d["a"] == 5  # f is called here again
    assert counter == 2

    # Normal mutation is not allowed.
    with pytest.raises(TypeError):
        del d["a"]
    with pytest.raises(TypeError):
        d["a"] = 5

    # Special mutation methods
    assert "a" in cache
    assert "a" in mapping
    assert "a" in d
    d.evict("a")
    assert "a" not in cache
    assert "a" in mapping
    assert "a" in d
    d.evict("a")  # idempotent
    assert "a" not in cache
    assert "a" in mapping
    assert "a" in d
    assert d["a"] == 5  # f is called here again
    assert counter == 3
    d.remove("a")
    assert "a" not in cache
    assert "a" not in mapping
    assert "a" not in d
    with pytest.raises(KeyError):
        d.remove("a")
    with pytest.raises(KeyError):
        d.remove("never existed")
    d.set("a", f)
    assert d["a"] == 5  # f is called here again
    assert counter == 4
    d.discard("a")
    assert "a" not in cache
    assert "a" not in mapping
    assert "a" not in d
    d.discard("a")  # idempotent
    d.discard("never existed")
