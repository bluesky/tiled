import collections.abc
import contextlib
import builtins
import enum
import importlib
import importlib.util
import operator
import os
import sys
import threading


class ListView(collections.abc.Sequence):
    "An immutable view of a list."

    def __init__(self, seq):
        self._internal_list = list(seq)

    def __repr__(self):
        return f"{type(self).__name__}({self._internal_list!r})"

    def __getitem__(self, index):
        return self._internal_list[index]

    def __iter__(self):
        yield from self._internal_list

    def __len__(self):
        return len(self._internal_list)

    def __setitem__(self, index, value):
        raise TypeError("Setting values is not allowed.")

    def __delitem__(self, index):
        raise TypeError("Deleting values is not allowed.")


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

    def set(self, key, value_factory):
        """
        Set key to a callable the returns value.
        """
        if not callable(value_factory):
            raise ValueError(
                "This requires a callable that return a value, not the value itself."
            )
        self.__mapping[key] = value_factory

    def discard(self, key):
        """
        Discard a key if it is present. This is idempotent.
        """
        self.__mapping.pop(key, None)
        self.evict(key)

    def remove(self, key):
        """
        Remove a key. Raises KeyError if key not present.
        """
        del self.__mapping[key]
        self.evict(key)

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
    Mapping that computes values on read and caches them in a configured cache.

    Parameters
    ----------
    mapping : dict-like
        Must map keys to callables that return values.
    cache : dict-like
        Will be used to cache values. May be ordinary dict, LRUCache, etc.
    """

    __slots__ = ("__mapping", "__cache")

    def __init__(self, mapping, cache):
        self.__mapping = mapping
        self.__cache = cache

    def __getitem__(self, key):
        try:
            return self.__cache[key]
        except KeyError:
            value = self.__mapping[key]()
            self.__cache[key] = value
            return value

    def set(self, key, value_factory):
        """
        Set key to a callable the returns value.
        """
        if not callable(value_factory):
            raise ValueError(
                "This requires a callable that return a value, not the value itself."
            )
        self.__mapping[key] = value_factory
        # This may be replacing (updating) an existing key. Clear any cached value.
        self.evict(key)

    def discard(self, key):
        """
        Discard a key if it is present. This is idempotent.
        """
        self.__mapping.pop(key, None)
        self.evict(key)

    def remove(self, key):
        """
        Remove a key. Raises KeyError if key not present.
        """
        del self.__mapping[key]
        self.evict(key)

    def evict(self, key):
        """
        Evict a key from the internal cache. This is idempotent.

        This does *not* remove the key from the mapping.
        If it is accessed, it will be recomputed and added back to the cache.
        """
        self.__cache.pop(key, None)

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


def walk(tree, nodes=None):
    "Walk the entries in a (nested) Tree depth first."
    if nodes is None:
        for node in tree:
            yield from walk(tree, [node])
    else:
        value = tree[nodes[-1]]
        if hasattr(value, "items"):
            yield nodes
            for k, v in value.items():
                yield from walk(value, nodes + [k])
        else:
            yield nodes


def gen_tree(tree, nodes=None, last=None):
    "A generator of lines for the tree utility"

    # Normally, traversing a Tree will cause the structure clients to be
    # instanitated which in turn triggers import of the associated libraries like
    # numpy, pandas, and xarray. We want to avoid paying for that, especially
    # when this function is used in a CLI where import overhead can accumulate to
    # about 2 seconds, the bulk of the time. Therefore, we do something a bit
    # "clever" here to override the normal structure clients with dummy placeholders.
    from .client.node import Node

    def dummy_client(*args, **kwargs):
        return None

    structure_clients = collections.defaultdict(lambda: dummy_client)
    structure_clients["node"] = Node
    fast_tree = tree.new_variation(structure_clients=structure_clients)
    if nodes is None:
        last_index = len(fast_tree) - 1
        for index, node in enumerate(fast_tree):
            yield from gen_tree(fast_tree, [node], [index == last_index])
    else:
        value = fast_tree[nodes[-1]]
        if hasattr(value, "items"):
            yield _line(nodes, last)
            last_index = len(value) - 1
            for index, (k, v) in enumerate(value.items()):
                yield from gen_tree(value, nodes + [k], last + [index == last_index])
        else:
            yield _line(nodes, last)


def tree(tree, max_lines=20):
    """
    Print a visual sketch of Tree structure akin to UNIX `tree`.

    Parameters
    ----------
    tree : Tree
    max_lines: int or None, optional
        By default, output is trucated at 20 lines. ``None`` means "Do not
        truncate."

    Examples
    --------

    >>> tree(tree)
    ├── A
    │   ├── dog
    │   ├── cat
    │   └── monkey
    └── B
        ├── snake
        ├── bear
        └── wolf

    """
    for counter, line in enumerate(gen_tree(tree), start=1):
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


def import_object(colon_separated_string, accept_live_object=False):
    if not isinstance(colon_separated_string, str):
        # We have been handed the live object itself.
        # Nothing to import. Pass it through.
        return colon_separated_string
    MESSAGE = (
        "Expected string formatted like:\n\n"
        "    package_name.module_name:object_name\n\n"
        "Notice *dots* between modules and a "
        f"*colon* before the object name. Received:\n\n{colon_separated_string!r}"
    )
    import_path, _, obj_path = colon_separated_string.partition(":")
    for segment in import_path.split("."):
        if not segment.isidentifier():
            raise ValueError(MESSAGE)
    for attr in obj_path.split("."):
        if not attr.isidentifier():
            raise ValueError(MESSAGE)
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


def parse(file):
    """
    Given a config file, parse it.

    This wraps YAML parsing and environment variable expansion.
    """
    import yaml

    content = yaml.safe_load(file.read())
    return expand_environment_variables(content)


def expand_environment_variables(config):
    """Expand environment variables in a nested config dictionary

    VENDORED FROM dask.config.

    This function will recursively search through any nested dictionaries
    and/or lists.

    Parameters
    ----------
    config : dict, iterable, or str
        Input object to search for environment variables

    Returns
    -------
    config : same type as input

    Examples
    --------
    >>> expand_environment_variables({'x': [1, 2, '$USER']})  # doctest: +SKIP
    {'x': [1, 2, 'my-username']}
    """
    if isinstance(config, collections.abc.Mapping):
        return {k: expand_environment_variables(v) for k, v in config.items()}
    elif isinstance(config, str):
        return os.path.expandvars(config)
    elif isinstance(config, (list, tuple, builtins.set)):
        return type(config)([expand_environment_variables(v) for v in config])
    else:
        return config


@contextlib.contextmanager
def prepend_to_sys_path(*paths):
    "Temporarily prepend items to sys.path."

    for item in reversed(paths):
        # Ensure item is str (not pathlib.Path).
        sys.path.insert(0, str(item))
    try:
        yield
    finally:
        for item in paths:
            sys.path.pop(0)


class MissingDependency(ModuleNotFoundError):
    pass


class UnrecognizedExtension(ValueError):
    pass


# The MIME type vnd.apache.arrow.file is provisional. See:
# https://lists.apache.org/thread.html/r9b462400a15296576858b52ae22e73f13c3e66f031757b2c9522f247%40%3Cdev.arrow.apache.org%3E  # noqa
# TODO Should we actually use vnd.apache.arrow.stream? I think 'file' is right
# for this use case but I have not read deeply into the details yet.
APACHE_ARROW_FILE_MIME_TYPE = "vnd.apache.arrow.file"
XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
