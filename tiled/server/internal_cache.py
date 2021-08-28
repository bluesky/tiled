"""
The 'internal' cache is available to all Adapters to cache their internal
resources, such as parsed file contents. It is a process-global singleton.
"""
import cachey


if __debug__:
    import logging

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setLevel("DEBUG")
    logger.addHandler(handler)


_internal_cache = None


def set_internal_cache(cache):
    """
    Set the process-global icache.
    """
    global _internal_cache
    _internal_cache = cache


def get_internal_cache():
    """
    Set the process-global icache.
    """
    return _internal_cache


class CacheInProcessMemory:
    def __init__(self, available_bytes):
        self.misses = 0
        self.hits = 0

        def miss(key):
            self.misses += 1

        def hit(key, value):
            self.hits += 1

        self._cache = cachey.Cache(available_bytes, 0, miss=miss, hit=hit)

    def get(self, key):
        value = self._cache.get(key)
        if __debug__:
            if value is None:
                logger.info("Internal cache miss %r", key)
            else:
                logger.info("Internal cache hit %r", key)
        return value

    def put(self, key, value, cost):
        if __debug__:
            logger.info("Internal cache store %r cost=%f.3", key, cost)
        self._cache.put(key, value, cost)
