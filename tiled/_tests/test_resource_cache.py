import cachetools

from ..adapters.resource_cache import default_resource_cache, with_resource_cache


def test_simple_cache() -> None:
    counter = 0

    def f() -> str:
        nonlocal counter
        counter += 1
        return "some value"

    cache = cachetools.Cache(maxsize=1)
    with_resource_cache("test_key", f, _resource_cache=cache)
    with_resource_cache("test_key", f, _resource_cache=cache)
    assert counter == 1


def test_default_cache() -> None:
    counter = 0

    def f() -> str:
        nonlocal counter
        counter += 1
        return "some value"

    cache = default_resource_cache()
    with_resource_cache("test_key", f, _resource_cache=cache)
    with_resource_cache("test_key", f, _resource_cache=cache)
    assert counter == 1


def test_cache_zero_size() -> None:
    counter = 0

    def f() -> str:
        nonlocal counter
        counter += 1
        return "some value"

    cache = cachetools.Cache(maxsize=0)
    with_resource_cache("test_key", f, _resource_cache=cache)
    with_resource_cache("test_key", f, _resource_cache=cache)
    assert counter == 2
