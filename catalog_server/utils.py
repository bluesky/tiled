import collections.abc
from functools import wraps
import threading


class DictView(collections.abc.Mapping):
    "An immutable view of a dict."

    def __init__(self, d):
        self._internal_dict = d

    def __repr__(self):
        return f"{type(self).__name__}({self._internal_dict!r})"

    def __getitem__(self, key):
        return self._internal_dict[key]

    def __iter__(self):
        yield from self._internal_dict

    def __len__(self):
        return len(self._internal_dict)

    def __setitem__(self, key, value):
        raise TypeError("Setting items is not allowed.")

    def __delitem__(self, key):
        raise TypeError("Deleting items is not allowed.")


# This object should never be directly instantiated by external code.
# It is defined at module scope because namedtupled need to be defined at
# module scope in order to be pickleable. It should only be used internally by
# LazyMap below.
_LazyMapWrapper = collections.namedtuple("_LazyMapWrapper", ("func",))


class LazyMap(collections.abc.Mapping):
    __slots__ = ("__mapping", "__lock")

    def __init__(self, *args, **kwargs):
        dictionary = dict(*args, **kwargs)
        wrap = _LazyMapWrapper
        # TODO should be recursive lock?
        self.__lock = threading.Lock()
        # TODO type validation?
        self.__mapping = {k: wrap(v) for k, v in dictionary.items()}

    def __getitem__(self, key):
        # TODO per-key locking?
        with self.__lock:
            v = self.__mapping[key]
            if isinstance(v, _LazyMapWrapper):
                # TODO handle exceptions?
                v = self.__mapping[key] = v.func()
        return v

    def __len__(self):
        return len(self.__mapping)

    def __iter__(self):
        return iter(self.__mapping)

    def __contains__(self, k):
        # make sure checking 'in' does not trigger evaluation
        return k in self.__mapping

    def __getstate__(self):
        return self.__mapping

    def __setstate__(self, mapping):
        self.__mapping = mapping
        self.__lock = threading.Lock()


class AuthenticationRequired(Exception):
    pass


def authenticated(method):
    @wraps(method)
    def inner(self, *args, **kwargs):
        if (self.access_policy is not None) and (self.authenticated_identity is None):
            raise AuthenticationRequired(
                f"Access policy on {self} is {self.access_policy}."
            )
        return method(self, *args, **kwargs)

    return inner


def slice_to_interval(index):
    "Check that slice is supported; then return (start, stop)."
    if index.start is None:
        start = 0
    elif index.start < 0:
        raise NotImplementedError
    else:
        start = index.start
    if index.stop is not None:
        if index.stop < 0:
            raise NotImplementedError
    stop = index.stop
    return start, stop


class IndexCallable:
    """Provide getitem syntax for functions

    >>> def inc(x):
    ...     return x + 1

    >>> I = IndexCallable(inc)
    >>> I[3]
    4

    Vendored from dask
    """

    __slots__ = ("fn",)

    def __init__(self, fn):
        self.fn = fn

    def __getitem__(self, key):
        return self.fn(key)
