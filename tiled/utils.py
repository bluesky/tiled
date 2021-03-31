import collections.abc
import enum
import importlib
import operator
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
# OneShotCachedMap below.
_OneShotCachedMapWrapper = collections.namedtuple("_OneShotCachedMapWrapper", ("func",))


class OneShotCachedMap(collections.abc.Mapping):
    __slots__ = ("__mapping", "__lock")

    def __init__(self, *args, **kwargs):
        dictionary = dict(*args, **kwargs)
        wrap = _OneShotCachedMapWrapper
        # TODO should be recursive lock?
        self.__lock = threading.Lock()
        # TODO type validation?
        self.__mapping = {k: wrap(v) for k, v in dictionary.items()}

    def __getitem__(self, key):
        # TODO per-key locking?
        with self.__lock:
            v = self.__mapping[key]
            if isinstance(v, _OneShotCachedMapWrapper):
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

    def __repr__(self):
        d = {}
        for k, v in self.__mapping.items():
            if isinstance(v, _OneShotCachedMapWrapper):
                d[k] = "<lazy>"
            else:
                d[k] = repr(v)
        return (
            f"<{type(self).__name__}"
            "({" + ", ".join(f"{k!r}: {v!s}" for k, v in d.items()) + "})>"
        )


class CachingMap(collections.abc.Mapping):
    """
    Mapping that computes values on read and optionally caches them.

    Parameters
    ----------
    mapping : dict-like
        Must map keys to callables that return values.
    cache : dict-like or None, optional
        Will be used to cache values. If None, nothing is cached.
        May be ordinary dict, LRUCache, etc.
    """

    __slots__ = ("__mapping", "__cache")

    def __init__(self, mapping, cache=None):
        self.__mapping = mapping
        self.__cache = cache

    def __getitem__(self, key):
        if self.__cache is None:
            return self.__mapping[key]()
        else:
            try:
                return self.__cache[key]
            except KeyError:
                value = self.__mapping[key]()
                self.__cache[key] = value
                return value

    def __len__(self):
        return len(self.__mapping)

    def __iter__(self):
        return iter(self.__mapping)

    def __contains__(self, key):
        # Ensure checking 'in' does not trigger evaluation.
        return key in self.__mapping

    def __getstate__(self):
        return self.__mapping, self.__cache

    def __setstate__(self, mapping, cache):
        self.__mapping = mapping
        self.__cache = cache

    def __repr__(self):
        if self.__cache is None:
            d = {k: "<lazy>" for k in self.__mapping}
        else:
            d = {}
            for k in self.__mapping:
                try:
                    value = self.__cache[k]
                except KeyError:
                    d[k] = "<lazy>"
                else:
                    d[k] = repr(value)
        return (
            f"<{type(self).__name__}"
            "({" + ", ".join(f"{k!r}: {v!s}" for k, v in d.items()) + "})>"
        )


class SpecialUsers(str, enum.Enum):
    public = "public"
    admin = "admin"


def _line(nodes, last):
    "Generate a single line for the tree utility"
    tee = "├"
    vertical = "│   "
    horizontal = "── "
    L = "└"
    blank = "    "
    indent = ""
    for item in last[:-1]:
        if item:
            indent += blank
        else:
            indent += vertical
    if last[-1]:
        return indent + L + horizontal + nodes[-1]
    else:
        return indent + tee + horizontal + nodes[-1]


def walk(catalog, nodes=None):
    "Walk the entries in a (nested) Catalog depth first."
    if nodes is None:
        for node in catalog:
            yield from walk(catalog, [node])
    else:
        value = catalog[nodes[-1]]
        if hasattr(value, "items"):
            yield nodes
            for k, v in value.items():
                yield from walk(value, nodes + [k])
        else:
            yield nodes


def _tree_gen(catalog, nodes=None, last=None):
    "A generator of lines for the tree utility"
    if nodes is None:
        last_index = len(catalog) - 1
        for index, node in enumerate(catalog):
            yield from _tree_gen(catalog, [node], [index == last_index])
    else:
        value = catalog[nodes[-1]]
        if hasattr(value, "items"):
            yield _line(nodes, last)
            last_index = len(value) - 1
            for index, (k, v) in enumerate(value.items()):
                yield from _tree_gen(value, nodes + [k], last + [index == last_index])
        else:
            yield _line(nodes, last)


def tree(catalog, max_lines=20):
    """
    Print a visual sketch of Catalog structure akin to UNIX `tree`.

    Parameters
    ----------
    catalog : Catalog
    max_lines: int or None, optional
        By default, output is trucated at 20 lines. ``None`` means "Do not
        truncate."

    Examples
    --------

    >>> tree(catalog)
    ├── A
    │   ├── dog
    │   ├── cat
    │   └── monkey
    └── B
        ├── snake
        ├── bear
        └── wolf

    """
    for counter, line in enumerate(_tree_gen(catalog), start=1):
        if (max_lines is not None) and (counter > max_lines):
            print(
                f"<Output truncated at {max_lines} lines. "
                "Adjust tree's max_lines parameter to see more.>"
            )
            break
        print(line)


class Sentinel:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"<{self.name}>"


def import_object(colon_separated_string):
    import_path, obj_path = colon_separated_string.split(":")
    module = importlib.import_module(import_path)
    return operator.attrgetter(obj_path)(module)


def modules_available(*module_names):
    for module_name in module_names:
        if not importlib.util.find_spec(module_name):
            break
    else:
        # All modules were found.
        return True
    return False
