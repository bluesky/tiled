import collections.abc
import inspect
import itertools

import mongoquery
from queries import MongoQuery


class QueryRegistry:
    def __init__(self):
        self._lookup = {}
        self._lazy = {}

    def register(self, class_, translator):
        self._lookup[class_] = translator
        return translator

    def register_lazy(self, toplevel, register):
        """
        Register a registration function which will be called if the
        *toplevel* module (e.g. 'pandas') is ever loaded.
        """
        self._lazy[toplevel] = register

    def dispatch(self, class_):
        """Return the function implementation for the given ``class_``"""
        # Fast path with direct lookup on cls
        lk = self._lookup
        try:
            impl = lk[class_]
        except KeyError:
            pass
        else:
            return impl
        # Is a lazy registration function present?
        toplevel, _, _ = class_.__module__.partition(".")
        try:
            register = self._lazy.pop(toplevel)
        except KeyError:
            pass
        else:
            register()
            return self.dispatch(class_)  # recurse
        # Walk the MRO and cache the lookup result
        for base in inspect.getmro(class_)[1:]:
            if base in lk:
                lk[class_] = lk[base]
                return lk[base]
        raise TypeError(f"No dispatch for {class_}")

    def __call__(self, arg, *args, **kwargs):
        """
        Call the corresponding method based on type of argument.
        """
        meth = self.dispatch(type(arg))
        return meth(arg, *args, **kwargs)


class Catalog(collections.abc.Mapping):

    # Define classmethods for managing what queries this Catalog knows.
    __query_registry = QueryRegistry()
    register_query = __query_registry.register_query
    deregister_query = __query_registry.deregister_query

    register_query(MongoQuery, lambda query: mongoquery.Query(query))

    def __init__(self, entries, metadata=None):
        self._entries = entries
        self._index_accessor = _IndexAccessor(entries, type(self))
        self._metadata = metadata or {}

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def __repr__(self):
        return f"{type(self).__name__}" "({...})"

    def __getitem__(self, key):
        return self._entries[key]

    def __iter__(self):
        yield from self._entries

    def __len__(self):
        return len(self._entries)

    def search(self, query):
        """
        Return a Catalog with a subset of the entries.

        >>> catalog.search(MongoQuery({"plan_name": "scan"}))
        Catalog({...})
        >>> catalog.search(TextSearch("Mn"))
        Catalog({...})
        """
        translated_query = self.__query_registry(type(query))
        return type(self)(
            {
                uid: run
                for uid, run in self.items()
                if translated_query.match(run.metadata["start"])
            }
        )

    @property
    def index(self):
        return self._index_accessor


class _IndexAccessor:
    "Internal object used by Catalog."

    def __init__(self, entries, out_type):
        self._entries = entries
        self._out_type = out_type

    def __getitem__(self, /, i):
        if isinstance(i, int):
            if i >= len(self._entries):
                raise IndexError("Catalog index out of range.")
            out = next(itertools.islice(self._entries.values(), i, 1 + i))
        elif isinstance(i, slice):
            out = self._out_type(
                dict(itertools.islice(self._entries.items(), i.start, i.stop, i.step))
            )
        else:
            raise TypeError("Catalog index must be integer or slice.")
        return out


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
