"""
The 'internal' cache is available to all Adapters to cache their internal
resources, such as parsed file contents. It is a process-global singleton.
"""

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
        import cachey

        self.misses = 0
        self.hits = 0

        def miss(key):
            self.misses += 1

        def hit(key, value):
            self.hits += 1

        self._cache = cachey.Cache(available_bytes, 0, miss=miss, hit=hit)

    def __getitem__(self, key):
        value = self._cache.get(key)
        if value is None:
            if __debug__:
                logger.info("Internal cache miss %r", key)
            raise KeyError(key)
        else:
            if __debug__:
                logger.info("Internal cache hit %r", key)
            return value

    def __setitem__(self, key, value):
        if __debug__:
            logger.info("Internal cache store %r", key)
        self._cache.put(key, value, cost=1)
