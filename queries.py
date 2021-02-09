"""
These objects express high-level queries and translate them (when possible)
into concrete queries for specific storage backends.

This intentionally only uses built-in dataclasses, not pydantic models.
"""
import collections.abc
from dataclasses import dataclass
import inspect


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


class QueryRegistry:
    """
    Keep track of all known queries types, with names.

    When the server starts up, it uses this registry to build routes for each
    type of query.

    There is a global instance of this, defined below. It is implemented as a
    class for the sake of tests.
    """

    def __init__(self):
        self._name_to_class = {}
        self._class_to_name = {}

    @property
    def queries_by_name(self):
        return DictView(self._name_to_class)

    @property
    def names_by_query_class(self):
        return DictView(self._class_to_name)

    def register(self, name=None, overwrite=False):
        def inner(cls):
            if (name in self._name_to_class) and (not overwrite):
                if self._name_to_class[name] is cls:
                    # redundant registration; do nothing
                    return
                raise Exception(
                    f"The class {self._lookup[name]} is registered to the "
                    f"name {name}. To overwrite, set overwrite=True."
                )
            if cls in self._name_to_class.values():
                raise Exception(
                    f"The class {cls} is already registered by another name."
                )
            self._name_to_class[name] = cls
            self._class_to_name[cls] = name
            return cls

        return inner


# Make a global registry.
_query_registry = QueryRegistry()
register = _query_registry.register
queries_by_name = _query_registry.queries_by_name
names_by_query_class = _query_registry.names_by_query_class


@register(name="text")
@dataclass
class Text:

    text: str


class QueryTranslationRegistry:
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
        print("lookup is", self._lookup)
        raise TypeError(f"No dispatch for {class_}")

    def __call__(self, arg, *args, **kwargs):
        """
        Call the corresponding method based on type of argument.
        """
        meth = self.dispatch(type(arg))
        return meth(arg, *args, **kwargs)
