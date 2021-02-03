"""
These objects express high-level queries and translate them (when possible)
into concrete queries for specific storage backends.
"""


class MongoQuery:
    def __init__(self, query):
        self._query = query

    def in_memory(self, catalog):
        # The mongoquery library is a pure-Python library that lets us do
        # MongoDB-like querys on Python collections, without an actual MongoDB.
        import importlib

        if not importlib.util.find_spec("mongoquery"):
            raise OptionalDependencyMissing(
                "mongoquery is required to search on this Catalog"
            )
        from mongoquery import Query

        parsed_query = Query(self._query)
        return type(catalog)(
            {
                uid: run
                for uid, run in catalog.items()
                if parsed_query.match(run.metadata["start"])
            }
        )

    def mongodb(self, catalog):
        return catalog.raw_search(self._query)


class TextSearch:
    def __init__(self, text):
        self._text = text

    def in_memory(self, catalog):
        # This is a rough best effort attempt. Might be better to just raise
        # NotImplementError on this one.
        return type(catalog)(
            {
                uid: run
                for uid, run in catalog.items()
                if self._text in repr(run.metadata["start"])
            }
        )

    def mongodb(self, catalog):
        return catalog.raw_search({"$text": {"$search": self._text}})


class OptionalDependencyMissing(ImportError):
    pass
