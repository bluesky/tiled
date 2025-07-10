import base64
import builtins
import collections.abc
import contextlib
import functools
import importlib
import importlib.util
import inspect
import operator
import os
import platform
import re
import sqlite3
import sys
import threading
import warnings
from collections import namedtuple
from pathlib import Path
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    Optional,
    TextIO,
    Tuple,
    TypeVar,
    Union,
)
from urllib.parse import urlparse, urlunparse

import anyio
import yaml

# helper for avoiding re-typing patch mimetypes
# namedtuple for the lack of StrEnum in py<3.11
patch_mimetypes = namedtuple(
    "patch_mimetypes",
    "JSON_PATCH MERGE_PATCH",
)(
    JSON_PATCH="application/json-patch+json",
    MERGE_PATCH="application/merge-patch+json",
)


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class ListView(collections.abc.Sequence[V], Generic[V]):
    "An immutable view of a list."

    def __init__(self, seq: collections.abc.Sequence[V]):
        self._internal_list = list(seq)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._internal_list!r})"

    def _repr_pretty_(self, p, cycle):
        """
        Provide "pretty" display in IPython/Jupyter.

        See https://ipython.readthedocs.io/en/stable/config/integrating.html#rich-display
        """
        from pprint import pformat

        list_ = self._internal_list
        # pformat only works on literal list, so convert if we have
        # some other list-like
        if not type(list_) is list:
            list_ = list(self._internal_list)
        p.text(pformat(list_))

    def __getitem__(self, index: int) -> V:
        return self._internal_list[index]

    def __iter__(self) -> collections.abc.Iterator[V]:
        yield from self._internal_list

    def __len__(self) -> int:
        return len(self._internal_list)

    def __setitem__(self, index: int, value: V):
        raise TypeError("Setting values is not allowed.")

    def __delitem__(self, index: int):
        raise TypeError("Deleting values is not allowed.")

    def __add__(self, other: collections.abc.Sequence[K]) -> "ListView[V | K]":
        return type(self)(self._internal_list + other)

    def __eq__(self, other: Any) -> bool:
        return self._internal_list == other


class DictView(collections.abc.Mapping[K, V], Generic[K, V]):
    "An immutable view of a dict."

    def __init__(self, d: collections.abc.Mapping[K, V]):
        self._internal_dict = d

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._internal_dict!r})"

    def _repr_pretty_(self, p, cycle):
        """
        Provide "pretty" display in IPython/Jupyter.

        See https://ipython.readthedocs.io/en/stable/config/integrating.html#rich-display
        """
        from pprint import pformat

        d = self._internal_dict
        # pformat only works on literal dict, so convert if we have
        # some other dict-like
        if not type(d) is dict:
            d = dict(self._internal_dict)
        p.text(pformat(d))

    def __getitem__(self, key: K) -> V:
        return self._internal_dict[key]

    def __iter__(self) -> collections.abc.Iterator[K]:
        yield from self._internal_dict

    def __len__(self) -> int:
        return len(self._internal_dict)

    def __setitem__(self, key: K, value: V):
        raise TypeError("Setting items is not allowed.")

    def __delitem__(self, key: K):
        raise TypeError("Deleting items is not allowed.")

    def __eq__(self, other: Any) -> bool:
        return self._internal_dict == other


# This object should never be directly instantiated by external code.
# It is defined at module scope because namedtupled need to be defined at
# module scope in order to be pickleable. It should only be used internally by
# OneShotCachedMap below.
_OneShotCachedMapWrapper = collections.namedtuple("_OneShotCachedMapWrapper", ("func",))


class OneShotCachedMap(collections.abc.Mapping[K, V], Generic[K, V]):
    __slots__ = ("__mapping", "__lock")

    def __init__(self, *args, **kwargs: Callable[[], V]):
        dictionary: dict[K, Union[Callable[[], V], V]] = dict(*args, **kwargs)
        wrap = _OneShotCachedMapWrapper
        # TODO should be recursive lock?
        self.__lock = threading.Lock()
        # TODO type validation?
        self.__mapping = {k: wrap(v) for k, v in dictionary.items()}

    def __getitem__(self, key: K) -> V:
        # TODO per-key locking?
        with self.__lock:
            v = self.__mapping[key]
            if isinstance(v, _OneShotCachedMapWrapper):
                # TODO handle exceptions?
                v = self.__mapping[key] = v.func()
        return v

    def set(self, key: K, value_factory: Callable[[], V]):
        """
        Set key to a callable the returns value.
        """
        if not callable(value_factory):
            raise ValueError(
                "This requires a callable that return a value, not the value itself."
            )
        self.__mapping[key] = _OneShotCachedMapWrapper(value_factory)

    def discard(self, key: K):
        """
        Discard a key if it is present. This is idempotent.
        """
        self.__mapping.pop(key, None)
        self.evict(key)

    def remove(self, key: K):
        """
        Remove a key. Raises KeyError if key not present.
        """
        del self.__mapping[key]
        self.evict(key)

    def __len__(self) -> int:
        return len(self.__mapping)

    def __iter__(self) -> collections.abc.Iterator[K]:
        return iter(self.__mapping)

    def __contains__(self, k: K) -> bool:
        # make sure checking 'in' does not trigger evaluation
        return k in self.__mapping

    def __getstate__(self) -> collections.abc.Mapping[K, Union[Callable[[], V], V]]:
        return self.__mapping

    def __setstate__(
        self, mapping: collections.abc.Mapping[K, Union[Callable[[], V], V]]
    ):
        self.__mapping = mapping
        self.__lock = threading.Lock()

    def __repr__(self) -> str:
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


class CachingMap(collections.abc.Mapping[K, V], Generic[K, V]):
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

    def __init__(
        self, mapping: collections.abc.Mapping[K, Callable[[], V]], cache: dict[K, V]
    ):
        self.__mapping = mapping
        self.__cache = cache

    def __getitem__(self, key: K) -> V:
        try:
            return self.__cache[key]
        except KeyError:
            value = self.__mapping[key]()
            self.__cache[key] = value
            return value

    def set(self, key: K, value_factory: Callable[[], V]):
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

    def discard(self, key: K):
        """
        Discard a key if it is present. This is idempotent.
        """
        self.__mapping.pop(key, None)
        self.evict(key)

    def remove(self, key: K):
        """
        Remove a key. Raises KeyError if key not present.
        """
        del self.__mapping[key]
        self.evict(key)

    def evict(self, key: K):
        """
        Evict a key from the internal cache. This is idempotent.

        This does *not* remove the key from the mapping.
        If it is accessed, it will be recomputed and added back to the cache.
        """
        self.__cache.pop(key, None)

    def __len__(self) -> int:
        return len(self.__mapping)

    def __iter__(self) -> collections.abc.Iterator[K]:
        return iter(self.__mapping)

    def __contains__(self, key: K) -> bool:
        # Ensure checking 'in' does not trigger evaluation.
        return key in self.__mapping

    def __getstate__(
        self,
    ) -> Tuple[collections.abc.Mapping[K, Callable[[], V]], dict[K, V]]:
        return self.__mapping, self.__cache

    def __setstate__(
        self, state: Tuple[collections.abc.Mapping[K, Callable[[], V]], dict[K, V]]
    ):
        mapping, cache = state
        self.__mapping = mapping
        self.__cache = cache

    def __repr__(self) -> str:
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
    # instantiated which in turn triggers import of the associated libraries like
    # numpy, pandas, and xarray. We want to avoid paying for that, especially
    # when this function is used in a CLI where import overhead can accumulate to
    # about 2 seconds, the bulk of the time. Therefore, we do something a bit
    # "clever" here to override the normal structure clients with dummy placeholders.
    from .client.container import Container

    def dummy_client(*args, **kwargs):
        return None

    structure_clients = collections.defaultdict(lambda: dummy_client)
    structure_clients["container"] = Container
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
        By default, output is truncated at 20 lines. ``None`` means "Do not
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
    if len(tree) == 0:
        print("<Empty>")
        return
    for counter, line in enumerate(gen_tree(tree), start=1):
        if (max_lines is not None) and (counter > max_lines):
            print(
                f"<Output truncated at {max_lines} lines. "
                "Adjust tree's max_lines parameter to see more.>"
            )
            break
        print(line)


class Sentinel:
    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"<{self.name}>"

    def __copy__(self) -> "Sentinel":
        # The goal here is to make copy.copy(sentinel) == sentinel
        return self

    def __deepcopy__(self, memo: "Sentinel") -> "Sentinel":
        # The goal here is to make copy.deepcopy(sentinel) == sentinel
        return self


UNCHANGED = Sentinel("UNCHANGED")
UNSET = Sentinel("UNSET")
SingleUserPrincipal = Sentinel("SingleUserPrincipal")


def import_object(colon_separated_string, accept_live_object=True):
    if not isinstance(colon_separated_string, str):
        if not accept_live_object:
            raise ValueError(
                "accept_live_object is False, but live object has been passed"
            )

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


def parse(file: TextIO) -> dict[Any, Any]:
    """
    Given a config file, parse it.

    This wraps YAML parsing and environment variable expansion.
    """
    import yaml

    content = yaml.safe_load(file.read())
    return expand_environment_variables(content)


def expand_environment_variables(config: T) -> T:
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
        # ignore type checks as type might change but it's still a mapping
        return {k: expand_environment_variables(v) for k, v in config.items()}  # type: ignore
    elif isinstance(config, str):
        return os.path.expandvars(config)
    elif isinstance(config, (list, tuple, builtins.set)):
        return type(config)([expand_environment_variables(v) for v in config])
    else:
        return config


@contextlib.contextmanager
def prepend_to_sys_path(*paths: Union[str, Path]) -> Iterator[None]:
    "Temporarily prepend items to sys.path."

    for item in reversed(paths):
        # Ensure item is str (not pathlib.Path).
        sys.path.insert(0, str(item))
    try:
        yield
    finally:
        for item in paths:
            sys.path.pop(0)


def safe_json_dump(content):
    """
    Baes64-encode raw bytes, and provide a fallback if orjson numpy handling fails.
    """
    import orjson

    def default(content):
        if isinstance(content, bytes):
            content = f"data:application/octet-stream;base64,{base64.b64encode(content).decode('utf-8')}"
            return content
        if isinstance(content, Path):
            return str(content)
        # No need to import numpy if it hasn't been used already.
        numpy = sys.modules.get("numpy", None)
        if numpy is not None:
            if isinstance(content, numpy.ndarray):
                # If we make it here, OPT_NUMPY_SERIALIZE failed because we have hit some edge case.
                # Give up on the numpy fast-path and convert to Python list.
                # If the items in this list aren't serializable (e.g. bytes) we'll recurse on each item.
                return content.tolist()
            elif isinstance(content, (bytes, numpy.bytes_)):
                return content.decode("utf-8")
        raise TypeError

    # Not all numpy dtypes are supported by orjson.
    # Fall back to converting to a (possibly nested) Python list.
    return orjson.dumps(content, option=orjson.OPT_SERIALIZE_NUMPY, default=default)


class MissingDependency(ModuleNotFoundError):
    pass


class UnrecognizedExtension(ValueError):
    pass


class SerializationError(Exception):
    pass


class UnsupportedShape(SerializationError):
    pass


class UnsupportedQueryType(TypeError):
    pass


class Conflicts(Exception):
    "Prompts the server to send 409 Conflicts with message"

    pass


class BrokenLink(Exception):
    "Prompts the server to send 410 Gone with message"

    pass


# Arrow obtained an official MIME type 2021-06-23.
# https://www.iana.org/assignments/media-types/application/vnd.apache.arrow.file
APACHE_ARROW_FILE_MIME_TYPE = "application/vnd.apache.arrow.file"
XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

"""Get the data files for this package."""


def get_share_tiled_path():
    """
    Walk up until we find share/tiled.

    Because it is outside the package, its location relative to us is up to the
    package manager. Rather than assuming that it is a specific number of
    directory levels above us, we walk upward until we find it. A file with
    long unique name helps us confirm that we have found the right directory.

    It's not clear whether being this flexible about the location of
    shared/tiled is really necessary, but JupyterHub uses a similar approach,
    so it seems possible that there is variation in the wild and that this is
    the right way to cope with that.
    """
    import sys
    from os.path import abspath, dirname, exists, join, split

    path = abspath(dirname(__file__))
    starting_points = [path]
    if not path.startswith(sys.prefix):
        starting_points.append(sys.prefix)
    for path in starting_points:
        # walk up, looking for prefix/share/tiled
        while path != "/":
            share_tiled = join(path, "share", "tiled")
            if exists(
                join(share_tiled, ".identifying_file_72628d5f953b4229b58c9f1f8f6a9a09")
            ):
                # We have the found the right directory,
                # or one that is trying very hard to pretend to be!
                return share_tiled
            path, _ = split(path)
    # Give up
    return ""


# Package managers can just override this with the appropriate constant
SHARE_TILED_PATH = get_share_tiled_path()


def node_repr(tree, sample) -> str:
    sample_reprs = list(map(repr, sample))
    out = f"<{type(tree).__name__} {{"
    # Always show at least one.
    if sample_reprs:
        out += sample_reprs[0]
    # And then show as many more as we can fit on one line.
    counter = 1
    for sample_repr in sample_reprs[1:]:
        if len(out) + len(sample_repr) > 60:  # character count
            break
        out += ", " + sample_repr
        counter += 1
    approx_len = operator.length_hint(tree)  # cheaper to compute than len(tree)
    # Are there more in the tree that what we displayed above?
    if approx_len > counter:
        out += f", ...}} ~{approx_len} entries>"
    else:
        out += "}>"
    return out


def bytesize_repr(num):
    # adapted from dask.utils
    for x in ["B", "KB", "MB", "GB", "TB"]:
        if num < 1024.0:
            if x == "B":
                s = f"{num:.0f} {x}"
            else:
                s = f"{num:.1f} {x}"
            return s
        num /= 1024.0


TIME_STRING_PATTERN = re.compile(r"(\d+)(s|m|h|d|y)")
TIME_STRING_UNITS = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 60 * 60 * 24,
    "y": 60 * 60 * 24 * 365,
}


def parse_time_string(s):
    """
    Accept strings like '1y', '1d', '24h'; return int seconds.

    Accepted Units:
    'y' = year
    'd' = day
    'h' = hour
    'm' = minutes
    's' = seconds
    """
    matched = TIME_STRING_PATTERN.match(s)
    if matched is None:
        raise ValueError(f"Could not parse {s} as a number and a unit like '5m'")
    number, unit = matched.groups()
    return int(number) * TIME_STRING_UNITS[unit]


def is_coroutine_callable(call: Callable[..., Any]) -> bool:
    if inspect.isroutine(call):
        return inspect.iscoroutinefunction(call)
    if inspect.isclass(call):
        return False
    dunder_call = getattr(call, "__call__", None)  # noqa: B004
    return inspect.iscoroutinefunction(dunder_call)


async def ensure_awaitable(func, *args, **kwargs):
    if is_coroutine_callable(func):
        return await func(*args, **kwargs)
    else:
        # run_sync() does not apply **kwargs to func
        # https://github.com/agronholm/anyio/issues/414
        return await anyio.to_thread.run_sync(functools.partial(func, **kwargs), *args)


def path_from_uri(uri) -> Path:
    """
    Give a URI, return a Path.

    If the URI has a scheme other than 'file', raise ValueError.

    >>> path_from_uri('file://localhost/a/b/c')  # POSIX-style
    '/a/b/c'
    >>> path_from_uri('file://localhost/C:/a/b/c')  # Windows-style
    'C:/a/b/c'
    """
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        if platform.system() == "Windows":
            # We slice because we need "C:/..." not "/C:/...".
            path = Path(parsed.path[1:])
        else:
            path = Path(parsed.path)
    elif parsed.scheme in {"sqlite", "duckdb"}:
        # The path begins after the third slash.
        path = Path(parsed.path[1:])
    else:
        raise ValueError(
            "Supported schemes are 'file', 'sqlite', and 'duckdb'. "
            f"Did not recognize scheme {parsed.scheme!r}"
        )
    return path


SCHEME_PATTERN = re.compile(r"^[a-z0-9+]+:\/\/.*$")


def ensure_uri(uri_or_path) -> str:
    "Accept a URI or file path (Windows- or POSIX-style) and return a URI."
    if not SCHEME_PATTERN.match(str(uri_or_path)):
        # Interpret this as a filepath.
        path = uri_or_path
        uri_str = urlunparse(
            ("file", "localhost", str(Path(path).absolute()), "", "", None)
        )
    else:
        # Interpret this as a URI.
        uri_str = uri_or_path
        parsed = urlparse(uri_str)
        if parsed.netloc == "":
            mutable = list(parsed)
            mutable[1] = "localhost"
            uri_str = urlunparse(mutable)
    return str(uri_str)


def sanitize_uri(uri: str) -> tuple[str, Optional[str], Optional[str]]:
    "Remove the username and password from uri if they are present."
    parsed_uri = urlparse(uri)
    netloc = parsed_uri.netloc
    if "@" in netloc:
        auth, netloc = netloc.split("@")
        # Remove username and password from the netloc.
        if ":" in auth:
            username, password = auth.split(":", 1)
        else:
            username, password = auth, None
        # Create clean components with the updated netloc
        clean_components = (
            parsed_uri.scheme,
            netloc,
            parsed_uri.path,
            parsed_uri.params,
            parsed_uri.query,
            parsed_uri.fragment,
        )
        return urlunparse(clean_components), username, password

    return uri, None, None


SCHEME_TO_SCHEME_PLUS_DRIVER = {
    "postgresql": "postgresql+asyncpg",
    "sqlite": "sqlite+aiosqlite",
}


def ensure_specified_sql_driver(uri: str) -> str:
    """
    Given a URI without a driver in the scheme, add Tiled's preferred driver.

    If a driver is already specified, the specified one will be used; it
    will NOT be overridden by this function.

    'postgresql://...' -> 'postgresql+asynpg://...'
    'sqlite://...' -> 'sqlite+aiosqlite://...'
    'postgresql+asyncpg://...' -> 'postgresql+asynpg://...'
    'postgresql+my_custom_driver://...' -> 'postgresql+my_custom_driver://...'
    '/path/to/file.db' -> 'sqlite+aiosqlite:////path/to/file.db'
    """
    if not SCHEME_PATTERN.match(str(uri)):
        # Interpret URI as filepath.
        uri = f"sqlite+aiosqlite:///{Path(uri)}"
    scheme, rest = uri.split(":", 1)
    new_scheme = SCHEME_TO_SCHEME_PLUS_DRIVER.get(scheme, scheme)
    return ":".join([new_scheme, rest])


class catch_warning_msg(warnings.catch_warnings):
    """Backward compatible version of catch_warnings for python <3.11.

    Allows regex matching of warning message, as in filterwarnings().
    Note that this context manager is not thread-safe.
    https://docs.python.org/3.12/library/warnings.html#warnings.catch_warnings
    """

    def __init__(
        self,
        *,
        action="",
        message="",
        category=Warning,
        module="",
        lineno=0,
        append=False,
        record=False,
    ):
        super().__init__(record=record)
        self.apply_filter = functools.partial(
            warnings.filterwarnings,
            action,
            message=message,
            category=category,
            module=module,
            lineno=lineno,
            append=append,
        )

    def __enter__(self):
        super().__enter__()
        self.apply_filter()
        return self


def parse_mimetype(mimetype: str) -> tuple[str, dict]:
    """
    Parse 'text/csv;header=absent' -> ('text/csv', {'header': 'absent'})
    """
    base, *param_tokens = mimetype.split(";")
    params = {}
    for item in param_tokens:
        try:
            key, value = item.strip().split("=", 2)
        except Exception:
            raise ValueError(
                f"Could not parse {item} as 'key=value' mimetype parameter"
            )
        params[key] = value
    return base, params


class InterningLoader(yaml.loader.BaseLoader):
    pass


def interning_constructor(loader, node):
    value = loader.construct_scalar(node)
    return sys.intern(value)


InterningLoader.add_constructor("tag:yaml.org,2002:str", interning_constructor)


def create_access_tags_tables(db):
    with contextlib.closing(db.cursor()) as cursor:
        tables_setup_sql = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS tags (
  id        INTEGER PRIMARY KEY,
  name      TEXT    UNIQUE NOT NULL,
  is_public INTEGER NOT NULL DEFAULT 0
    CHECK (is_public IN (0,1))
);
CREATE TABLE IF NOT EXISTS users (
  id   INTEGER PRIMARY KEY,
  name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS scopes (
  id   INTEGER PRIMARY KEY,
  name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS tags_users_scopes (
  tag_id    INTEGER NOT NULL
    REFERENCES tags(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  user_id   INTEGER NOT NULL
    REFERENCES users(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  scope_id  INTEGER NOT NULL
    REFERENCES scopes(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  PRIMARY KEY (tag_id, user_id, scope_id)
);
CREATE TABLE IF NOT EXISTS tag_owners (
  tag_id   INTEGER NOT NULL
    REFERENCES tags(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  user_id  INTEGER NOT NULL
    REFERENCES users(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  PRIMARY KEY (tag_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_tags_is_public ON tags (is_public);
CREATE INDEX IF NOT EXISTS idx_tus_users_scopes ON tags_users_scopes (user_id, scope_id);
CREATE INDEX IF NOT EXISTS idx_tus_users_scopes_scopeid ON tags_users_scopes (scope_id);
CREATE INDEX IF NOT EXISTS idx_tag_owners ON tag_owners (user_id);
CREATE VIEW IF NOT EXISTS public_tags AS
  SELECT name
  FROM tags
  WHERE is_public = 1;
CREATE VIEW IF NOT EXISTS user_tag_scopes AS
  SELECT
    u.name AS user_name,
    t.name AS tag_name,
    s.name AS scope_name
  FROM tags_users_scopes tus
    JOIN users  u ON u.id = tus.user_id
    JOIN tags   t ON t.id = tus.tag_id
    JOIN scopes s ON s.id = tus.scope_id;
CREATE VIEW IF NOT EXISTS user_tag_owners AS
  SELECT
    u.name AS user_name,
    t.name AS tag_name
  FROM tag_owners towner
    JOIN users u ON u.id = towner.user_id
    JOIN tags  t ON t.id = towner.tag_id;
PRAGMA optimize;
"""
        cursor.executescript(tables_setup_sql)
        db.commit()


def update_access_tags_tables(db, scopes, tags, owners, public_tags):
    with contextlib.closing(db.cursor()) as cursor:
        tables_stage_sql = """
BEGIN TRANSACTION;
CREATE TEMP TABLE IF NOT EXISTS stage_tags (
  id        INTEGER,
  name      TEXT    NOT NULL,
  is_public INTEGER NOT NULL
    CHECK (is_public IN (0,1))
);
CREATE TEMP TABLE IF NOT EXISTS stage_users (
  id   INTEGER,
  name TEXT NOT NULL
);
CREATE TEMP TABLE IF NOT EXISTS stage_scopes (
  id   INTEGER,
  name TEXT NOT NULL
);
CREATE TEMP TABLE IF NOT EXISTS stage_tags_users_scopes (
  tag_id   INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  scope_id INTEGER NOT NULL
);
CREATE TEMP TABLE IF NOT EXISTS stage_tag_owners (
  tag_id  INTEGER NOT NULL,
  user_id INTEGER NOT NULL
);
"""
        cursor.executescript(tables_stage_sql)

        # put all items into staging
        all_tags = [(tag, 0) for tag in tags] + [(tag, 0) for tag in owners]
        all_public = [(tag,) for tag in public_tags]
        all_users = {(user,) for users in tags.values() for user in users}
        all_users.update({(user,) for users in owners.values() for user in users})
        all_scopes = [(scope,) for scope in scopes]
        cursor.executemany(
            "INSERT INTO stage_tags(name, is_public) VALUES (?,?);",
            all_tags,
        )
        cursor.executemany(
            "UPDATE stage_tags SET is_public = 1 WHERE name = (?);", all_public
        )
        cursor.executemany(
            "INSERT INTO stage_users(name) VALUES (?);",
            all_users,
        )
        cursor.executemany(
            "INSERT INTO stage_scopes(name) VALUES (?);",
            all_scopes,
        )

        # push item names and metadata from staging to prod
        # then pull back ID values from prod to staging
        # note that UPSERT should always have a WHERE clause
        #   to avoid ambiguity. See the SQLite docs section 2.2
        #   https://www.sqlite.org/lang_upsert.html
        stage_push_sql = """
BEGIN TRANSACTION;
INSERT INTO tags (name, is_public)
  SELECT name, is_public
  FROM stage_tags
  WHERE true
  ON CONFLICT (name)
  DO UPDATE SET is_public = excluded.is_public;
INSERT INTO users(name) SELECT name FROM stage_users WHERE true ON CONFLICT(name) DO NOTHING;
INSERT INTO scopes(name) SELECT name FROM stage_scopes WHERE true ON CONFLICT(name) DO NOTHING;
UPDATE stage_tags SET id = (SELECT id FROM tags WHERE tags.name = stage_tags.name);
UPDATE stage_users SET id = (SELECT id FROM users WHERE users.name = stage_users.name);
UPDATE stage_scopes SET id = (SELECT id FROM scopes WHERE scopes.name = stage_scopes.name);
"""
        cursor.executescript(stage_push_sql)

        # load db IDs for items into memory
        cursor.execute("SELECT id, name FROM stage_tags")
        tags_to_id = {sys.intern(name): tag_id for (tag_id, name) in cursor.fetchall()}
        cursor.execute("SELECT id, name FROM stage_users")
        users_to_id = {
            sys.intern(name): user_id for (user_id, name) in cursor.fetchall()
        }
        cursor.execute("SELECT id, name FROM stage_scopes")
        scopes_to_id = {
            sys.intern(name): scope_id for (scope_id, name) in cursor.fetchall()
        }

        # flatten relationships and push to staging
        tags_users_scopes = [
            (tags_to_id[tag], users_to_id[user], scopes_to_id[scope])
            for tag, users in tags.items()
            for user, scopes in users.items()
            for scope in scopes
        ]
        tag_owners = [
            (tags_to_id[tag], users_to_id[user])
            for tag, users in owners.items()
            for user in users
        ]
        cursor.executemany(
            "INSERT INTO stage_tags_users_scopes(tag_id, user_id, scope_id) VALUES (?,?,?);",
            tags_users_scopes,
        )
        cursor.executemany(
            "INSERT INTO stage_tag_owners(tag_id, user_id) VALUES (?,?);", tag_owners
        )

        # delete outdated tags from prod and add updated relationships to db
        # finally, drop the staging tables
        # to-do: consider refactoring this to indvidual execute statements
        #        to avoid implicit pre-mature commit by executescript()
        upsert_delete_sql = """
BEGIN TRANSACTION;
DELETE from tags WHERE id NOT in (SELECT id FROM stage_tags);
DELETE from users WHERE id NOT in (SELECT id FROM stage_users);
DELETE from scopes WHERE id NOT in (SELECT id FROM stage_scopes);
INSERT INTO tags_users_scopes (tag_id, user_id, scope_id)
  SELECT tag_id, user_id, scope_id FROM stage_tags_users_scopes
  WHERE true
  ON CONFLICT (tag_id, user_id, scope_id) DO NOTHING;
DELETE FROM tags_users_scopes
  WHERE (tag_id, user_id, scope_id)
  NOT IN (SELECT tag_id, user_id, scope_id FROM stage_tags_users_scopes);
INSERT INTO tag_owners (tag_id, user_id)
  SELECT tag_id, user_id FROM stage_tag_owners
  WHERE true
  ON CONFLICT (tag_id, user_id) DO NOTHING;
DELETE FROM tag_owners
  WHERE (tag_id, user_id) NOT IN (SELECT tag_id, user_id FROM stage_tag_owners);
DROP TABLE IF EXISTS stage_tags;
DROP TABLE IF EXISTS stage_users;
DROP TABLE IF EXISTS stage_scopes;
DROP TABLE IF EXISTS stage_tags_users_scopes;
DROP TABLE IF EXISTS stage_tag_owners;
PRAGMA optimize;
"""
        cursor.executescript(upsert_delete_sql)
        db.commit()


class AccessTagsCompiler:
    _MAX_TAG_NESTING = 5

    def __init__(
        self,
        scopes,
        tag_config,
        tags_db,
        group_parser,
    ):
        self.scopes = scopes or {}
        self.tag_config_path = Path(tag_config)
        self.connection = sqlite3.connect(
            tags_db["uri"], uri=True, check_same_thread=False
        )
        self.group_parser = group_parser

        self.max_tag_nesting = max(self._MAX_TAG_NESTING, 0)
        self.public_tag = sys.intern("public".casefold())

        self.roles = {}
        self.tags = {}
        self.tag_owners = {}
        self.compiled_tags = {self.public_tag: {}}
        self.compiled_public = set({self.public_tag})
        self.compiled_tag_owners = {}

        create_access_tags_tables(self.connection)

    def load_tag_config(self):
        try:
            with open(self.tag_config_path) as tag_config:
                tag_definitions = yaml.load(tag_config, Loader=InterningLoader)
                self.roles.update(tag_definitions.get("roles", {}))
                self.tags.update(tag_definitions["tags"])
                self.tag_owners.update(tag_definitions.get("tag_owners", {}))
        except FileNotFoundError as e:
            raise ValueError(
                f"The tag config path {self.tag_config_path!s} doesn't exist."
            ) from e

    def _dfs(self, current_tag, tags, seen_tags, nested_level=0):
        if current_tag in self.compiled_tags:
            return self.compiled_tags[current_tag], current_tag in self.compiled_public
        if current_tag in seen_tags:
            return {}, False
        if nested_level > self.max_tag_nesting:
            raise RecursionError(
                f"Exceeded maximum tag nesting of {self.max_tag_nesting} levels"
            )

        public_auto_tag = False
        seen_tags.add(current_tag)
        users = {}
        for tag in tags[current_tag]:
            if tag.casefold() == self.public_tag:
                public_auto_tag = True
                continue
            try:
                child_users, child_public = self._dfs(
                    tag, tags, seen_tags, nested_level + 1
                )
                public_auto_tag = public_auto_tag or child_public
                users.update(child_users)
            except (RecursionError, ValueError) as e:
                raise RuntimeError(
                    f"Tag compilation failed at tag: {current_tag}"
                ) from e

        if public_auto_tag:
            self.compiled_public.add(current_tag)

        if "users" in self.tags[current_tag]:
            for user in self.tags[current_tag]["users"]:
                username = user["name"]
                if all(k in user for k in ("scopes", "role")):
                    raise ValueError(
                        f"Cannot define both 'scopes' and 'role' for a user. {username=}"
                    )
                elif not any(k in user for k in ("scopes", "role")):
                    raise ValueError(
                        f"Must define either 'scopes' or 'role' for a user. {username=}"
                    )

                user_scopes = set(
                    self.roles[user["role"]]["scopes"]
                    if ("role" in user) and (user["role"] in self.roles)
                    else user.get("scopes", [])
                )
                if not user_scopes:
                    raise ValueError(f"Scopes must not be empty. {username=}")
                if not user_scopes.issubset(self.scopes):
                    raise ValueError(
                        f"Scopes for {username=} are not in the valid set of scopes. The invalid scopes are:"
                        f"{user_scopes.difference(self.scopes)}"
                    )
                users.setdefault(username, set())
                users[username].update(user_scopes)

        if "groups" in self.tags[current_tag]:
            for group in self.tags[current_tag]["groups"]:
                groupname = group["name"]
                if all(k in group for k in ("scopes", "role")):
                    raise ValueError(
                        f"Cannot define both 'scopes' and 'role' for a group. {groupname=}"
                    )
                elif not any(k in group for k in ("scopes", "role")):
                    raise ValueError(
                        f"Must define either 'scopes' or 'role' for a group. {groupname=}"
                    )

                group_scopes = set(
                    self.roles[group["role"]]["scopes"]
                    if ("role" in group) and (group["role"] in self.roles)
                    else group.get("scopes", [])
                )
                if not group_scopes:
                    raise ValueError(f"Scopes must not be empty. {groupname=}")
                if not group_scopes.issubset(self.scopes):
                    raise ValueError(
                        f"Scopes for {groupname=} are not in the valid set of scopes. The invalid scopes are:"
                        f"{group_scopes.difference(self.scopes)}"
                    )

                try:
                    usernames = self.group_parser(groupname)
                except KeyError:
                    warnings.warn(
                        f"Group with {groupname=} does not exist - skipping",
                        UserWarning,
                    )
                    continue
                else:
                    for username in usernames:
                        username = sys.intern(username)
                        users.setdefault(username, set())
                        users[username].update(group_scopes)

        self.compiled_tags[current_tag] = users
        return users, public_auto_tag

    def compile(self):
        for role in self.roles.values():
            if "scopes" not in role:
                raise ValueError(f"Scopes must be defined for a role. {role=}")
            if not role["scopes"]:
                raise ValueError(f"Scopes must not be empty. {role=}")
            if not set(role["scopes"]).issubset(self.scopes):
                raise ValueError(
                    f"Scopes for {role=} are not in the valid set of scopes. The invalid scopes are:"
                    f'{set(role["scopes"]).difference(self.scopes)}'
                )

        adjacent_tags = {}
        for tag, members in self.tags.items():
            if tag.casefold() == self.public_tag:
                raise ValueError(
                    f"'Public' tag '{self.public_tag}' cannot be redefined."
                )
            adjacent_tags[tag] = set()
            if "auto_tags" in members:
                for auto_tag in members["auto_tags"]:
                    if (
                        auto_tag["name"] not in self.tags
                        and auto_tag["name"].casefold() != self.public_tag
                    ):
                        raise KeyError(
                            f"Tag '{tag}' has nested tag '{auto_tag}' which does not have a definition."
                        )
                    adjacent_tags[tag].add(auto_tag["name"])

        for tag in adjacent_tags:
            try:
                self._dfs(tag, adjacent_tags, set())
            except (RecursionError, ValueError) as e:
                raise RuntimeError(f"Tag compilation failed at tag: {tag}") from e

        for tag in self.tag_owners:
            self.compiled_tag_owners.setdefault(tag, set())
            if "users" in self.tag_owners[tag]:
                for user in self.tag_owners[tag]["users"]:
                    username = user["name"]
                    self.compiled_tag_owners[tag].add(username)
            if "groups" in self.tag_owners[tag]:
                for group in self.tag_owners[tag]["groups"]:
                    groupname = group["name"]
                    try:
                        usernames = self.group_parser(groupname)
                    except KeyError:
                        warnings.warn(
                            f"Group with {groupname=} does not exist - skipping",
                            UserWarning,
                        )
                        continue
                    else:
                        for username in usernames:
                            username = sys.intern(username)
                            self.compiled_tag_owners[tag].add(username)

        update_access_tags_tables(
            self.connection,
            self.scopes,
            self.compiled_tags,
            self.compiled_tag_owners,
            self.compiled_public,
        )

    def clear_raw_tags(self):
        self.roles = {}
        self.tags = {}
        self.tag_owners = {}

    def recompile(self):
        self.compiled_tags = {self.public_tag: {}}
        self.compiled_public = set({self.public_tag})
        self.compiled_tag_owners = {}
        self.compile()
>>>>>>> 49d7ac9f (Move shareable authz utilities into Tiled)
