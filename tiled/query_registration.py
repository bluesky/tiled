"""
These objects express high-level queries and translate them (when possible)
into concrete queries for specific storage backends.

This intentionally only uses built-in dataclasses, not pydantic models.
"""
import inspect
from dataclasses import fields

from .utils import DictView


class QueryRegistry:
    """
    Keep track of all known queries types, with names.

    When the server starts up, it uses this registry to populate the list of
    allowed URL query parameters to its /search route.

    There is a global instance of this, defined below. It is implemented as a
    class, rather than a module-scope singleton, for the sake of tests.
    """

    def __init__(self):
        self._name_to_query_type_type = {}
        self._query_type_to_name = {}

    @property
    def name_to_query_type(self):
        return DictView(self._name_to_query_type_type)

    @property
    def query_type_to_name(self):
        return DictView(self._query_type_to_name)

    def register(self, name=None, overwrite=False, must_revalidate=True):
        """
        Register a new type of query.
        """
        if "___" in name:
            raise Exception("Names must not contain triple underscores ('___').")
            # Why? This would create ambiguity in the server's handling of
            # search requests. Route signature parameters are named like
            # "filter___{name}___{field}".

        def inner(cls):
            if (name in self._name_to_query_type_type) and (not overwrite):
                if self._name_to_query_type_type[name] is cls:
                    # redundant registration; do nothing
                    return
                raise Exception(
                    f"The class {self._name_to_query_type_type[name]} is registered to the "
                    f"name {name}. To overwrite, set overwrite=True."
                )
            if cls in self._name_to_query_type_type.values():
                raise Exception(
                    f"The class {cls} is already registered by another name."
                )
            for field in fields(cls):
                if "___" in field.name:
                    raise Exception(
                        "Fields must not contain triple underscores ('___')."
                    )
                    # Why? This would create ambiguity in the server's handling of
                    # search requests. Route signature parameters are named like
                    # "filter___{name}___{field}".
            if cls in self._query_type_to_name:
                raise Exception(
                    f"The type {cls} is already registered to the name "
                    f"{self._query_type_to_name[cls]} and cannot also be "
                    f"registered to {name}."
                )
            self._name_to_query_type_type[name] = cls
            self._query_type_to_name[cls] = name
            return cls

        return inner


# Make a global registry.
query_registry = QueryRegistry()
register = query_registry.register
"""Register a new type of query."""


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
        raise TypeError(f"No dispatch for {class_}")

    def __call__(self, arg, *args, **kwargs):
        """
        Call the corresponding method based on type of argument.
        """
        meth = self.dispatch(type(arg))
        return meth(arg, *args, **kwargs)
