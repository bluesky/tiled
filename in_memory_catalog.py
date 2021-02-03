import collections.abc
import itertools


class Catalog(collections.abc.Mapping):
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
        return query.in_memory(self)

    @property
    def index(self):
        return self._index_accessor


class _IndexAccessor:
    def __init__(self, entries, out_type):
        self._entries = entries
        self._out_type = out_type

    def __getitem__(self, /, i):
        if isinstance(i, int):
            if i >= len(self._entries):
                raise IndexError("Catalog index out of range.")
            out = next(itertools.islice(self._entries.items(), i, 1 + i))
        elif isinstance(i, slice):
            out = itertools.islice(self._entries.items(), i.start, i.stop, i.step)
        else:
            raise TypeError("Catalog index must be integer or slice.")
        return self._out_type(dict(out))


class DictView(collections.abc.Mapping):
    "An immutable view of a dict."

    def __init__(self, d):
        self._internal_dict = d

    def __repr__(self):
        return f"{self.__class__.__name__}({self._internal_dict!r})"

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
