import collections.abc
from dataclasses import dataclass

import pymongo

from catalog_server.query_registration import QueryTranslationRegistry, register
from catalog_server.queries import FullText, KeyLookup
from catalog_server.utils import (
    authenticated,
    catalog_repr,
    DictView,
    IndexCallable,
    slice_to_interval,
    SpecialUsers,
)
from catalog_server.catalogs.in_memory import Catalog as CatalogInMemory


class BlueskyRun(CatalogInMemory):
    client_type_hint = "BlueskyRun"

    def __repr__(self):
        return f"<{type(self).__name__}(uid={self.metadata['start']['uid']})>"

    def documents(self):
        yield ("start", self.metadata["start"])
        stop_doc = self.metadata["stop"]
        if stop_doc is not None:
            yield ("stop", stop_doc)


class Catalog(collections.abc.Mapping):

    # Define classmethods for managing what queries this Catalog knows.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    def __init__(
        self,
        metadatastore_db,
        asset_registry_db,
        metadata=None,
        queries=None,
        access_policy=None,
        authenticated_identity=None,
    ):
        self._run_start_collection = metadatastore_db.get_collection("run_start")
        self._run_stop_collection = metadatastore_db.get_collection("run_stop")
        self._event_descriptor_collection = metadatastore_db.get_collection(
            "event_descriptor"
        )
        self._event_collection = metadatastore_db.get_collection("event")
        self._resource_collection = asset_registry_db.get_collection("resource")
        self._datum_collection = asset_registry_db.get_collection("datum")
        self._metadatastore_db = metadatastore_db
        self._asset_registry_db = asset_registry_db

        self._metadata = metadata or {}
        self._high_level_queries = tuple(queries or [])
        self._mongo_queries = [self.query_registry(q) for q in self._high_level_queries]
        if (access_policy is not None) and (
            not access_policy.check_compatibility(self)
        ):
            raise ValueError(
                f"Access policy {access_policy} is not compatible with this Catalog."
            )
        self._access_policy = access_policy
        self._authenticated_identity = authenticated_identity
        self.keys_indexer = IndexCallable(self._keys_indexer)
        self.items_indexer = IndexCallable(self._items_indexer)
        self.values_indexer = IndexCallable(self._values_indexer)

    @property
    def metadatastore_db(self):
        return self._metadatastore_db

    @property
    def asset_registry_db(self):
        return self._asset_registry_db

    @classmethod
    def from_uri(
        cls,
        metadatastore,
        asset_registry=None,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
    ):
        metadatastore_db = _get_database(metadatastore)
        if asset_registry is None:
            asset_registry_db = metadatastore_db
        else:
            asset_registry_db = _get_database(asset_registry)
        return cls(
            metadatastore_db=metadatastore_db,
            asset_registry_db=asset_registry_db,
            metadata=metadata,
            access_policy=access_policy,
            authenticated_identity=authenticated_identity,
        )

    @property
    def access_policy(self):
        return self._access_policy

    @property
    def authenticated_identity(self):
        return self._authenticated_identity

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def __repr__(self):
        # Display the first N keys to avoid making a giant service request.
        # Use _keys_slicer because it is unauthenticated.
        N = 5
        return catalog_repr(self, self._keys_slice(0, N))

    @authenticated
    def __getitem__(self, key):
        # Lookup this key *within the search results* of this Catalog.
        query = self._mongo_query({"uid": key})
        run_start_doc = self._run_start_collection.find_one(query, {"_id": False})
        if run_start_doc is None:
            raise KeyError(key)
        # This may be None; that's fine.
        run_stop_doc = self._get_stop_doc(run_start_doc["uid"])
        return BlueskyRun(
            {}, metadata={"start": run_start_doc, "stop": run_stop_doc}  # TODO
        )

    def _chunked_find(self, collection, query, *args, skip=0, limit=None, **kwargs):
        # This is an internal chunking that affects how much we pull from
        # MongoDB at a time.
        CURSOR_LIMIT = 100  # TODO Tune this for performance.
        if limit is not None and limit < CURSOR_LIMIT:
            initial_limit = limit
        else:
            initial_limit = CURSOR_LIMIT
        cursor = (
            collection.find(query, *args, **kwargs)
            .sort([("_id", 1)])
            .skip(skip)
            .limit(initial_limit)
        )
        # Fetch in batches, starting each batch from where we left off.
        # https://medium.com/swlh/mongodb-pagination-fast-consistent-ece2a97070f3
        tally = 0
        items = []
        while True:
            # Greedily exhaust the cursor. The user may loop over this iterator
            # slowly and, if we don't pull it all into memory now, we'll be
            # holding a long-lived cursor that might get timed out by the
            # MongoDB server.
            items.extend(cursor)  # Exhaust cursor.
            if not items:
                break
            # Next time through the loop, we'll pick up where we left off.
            last_object_id = items[-1]["_id"]
            query["_id"] = {"$gt": last_object_id}
            for item in items:
                item.pop("_id")
                yield item
            tally += len(items)
            if limit is not None:
                if tally == limit:
                    break
                if limit - tally < CURSOR_LIMIT:
                    this_limit = limit - tally
                else:
                    this_limit = CURSOR_LIMIT
            else:
                this_limit = CURSOR_LIMIT
            # Get another batch and go round again.
            cursor = (
                collection.find(query, *args, **kwargs)
                .sort([("_id", 1)])
                .limit(this_limit)
            )
            items.clear()

    def _mongo_query(self, *queries):
        combined = self._mongo_queries + list(queries)
        if combined:
            return {"$and": combined}
        else:
            return {}

    def _get_stop_doc(self, run_start_uid):
        "This may return None."
        return self._run_stop_collection.find_one(
            {"run_start": run_start_uid}, {"_id": False}
        )

    @authenticated
    def __iter__(self):
        for run_start_doc in self._chunked_find(
            self._run_start_collection, self._mongo_query()
        ):
            yield run_start_doc["uid"]

    @authenticated
    def __len__(self):
        return self._run_start_collection.count_documents(self._mongo_query())

    def __length_hint__(self):
        # https://www.python.org/dev/peps/pep-0424/
        return self._run_start_collection.estimated_document_count(
            self._mongo_query(),
        )

    def authenticated_as(self, identity):
        if self._authenticated_identity is not None:
            raise RuntimeError(
                f"Already authenticated as {self.authenticated_identity}"
            )
        if self._access_policy is not None:
            raise NotImplementedError
        else:
            catalog = type(self)(
                metadatastore_db=self.metadatastore_db,
                asset_registry_db=self.asset_registry_db,
                queries=self._high_level_queries,
                metadata=self.metadata,
                access_policy=self.access_policy,
                authenticated_identity=self.authenticated_identity,
            )
        return catalog

    @authenticated
    def search(self, query):
        """
        Return a Catalog with a subset of the mapping.
        """
        return type(self)(
            self._metadatastore_db,
            self._asset_registry_db,
            metadata=self.metadata,
            queries=self._high_level_queries + (query,),
            access_policy=self.access_policy,
            authenticated_identity=self.authenticated_identity,
        )

    def _keys_slice(self, start, stop):
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None
        for run_start_doc in self._chunked_find(
            self._run_start_collection, self._mongo_query(), skip=skip, limit=limit
        ):
            # TODO Fetch just the uid.
            yield run_start_doc["uid"]

    def _items_slice(self, start, stop):
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None
        for run_start_doc in self._chunked_find(
            self._run_start_collection, self._mongo_query(), skip=skip, limit=limit
        ):
            # This may be None; that's fine.
            run_stop_doc = self._get_stop_doc(run_start_doc["uid"])
            yield (
                run_start_doc["uid"],
                BlueskyRun(
                    {}, metadata={"start": run_start_doc, "stop": run_stop_doc}  # TODO
                ),
            )

    def _item_by_index(self, index):
        if index >= len(self):
            raise IndexError(f"index {index} out of range for length {len(self)}")
        run_start_doc = next(
            self._chunked_find(
                self._run_start_collection, self._mongo_query(), skip=index, limit=1
            )
        )
        # This may be None; that's fine.
        run_stop_doc = self._get_stop_doc(run_start_doc["uid"])
        key = run_start_doc["uid"]
        value = BlueskyRun(
            {},  # TODO
            metadata={"start": run_start_doc, "stop": run_stop_doc},
        )
        return (key, value)

    @authenticated
    def _keys_indexer(self, index):
        if isinstance(index, int):
            key, _value = self._item_by_index(index)
            return key
        elif isinstance(index, slice):
            start, stop = slice_to_interval(index)
            return list(self._keys_slice(start, stop))
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")

    @authenticated
    def _items_indexer(self, index):
        if isinstance(index, int):
            return self._item_by_index(index)
        elif isinstance(index, slice):
            start, stop = slice_to_interval(index)
            return list(self._items_slice(start, stop))
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")

    @authenticated
    def _values_indexer(self, index):
        if isinstance(index, int):
            _key, value = self._item_by_index(index)
            return value
        elif isinstance(index, slice):
            start, stop = slice_to_interval(index)
            return [value for _key, value in self._items_slice(start, stop)]
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")


def full_text_search(query):
    return Catalog.query_registry(RawMongo(start={"$text": {"$search": query.text}}))


def key_lookup(query):
    return Catalog.query_registry(RawMongo(start={"$uid": query.uid}))


def raw_mongo(query):
    # For now, only handle search on the 'run_start' collection.
    return query.start


@register(name="raw_mongo")
@dataclass
class RawMongo:
    """
    Run a MongoDB query against a given collection.
    """

    start: dict


Catalog.register_query(FullText, full_text_search)
Catalog.register_query(KeyLookup, key_lookup)
Catalog.register_query(RawMongo, raw_mongo)


class DummyAccessPolicy:
    "Impose no access restrictions."

    def check_compatibility(self, catalog):
        # This only works on in-memory Catalog or subclases.
        return isinstance(catalog, Catalog)

    def modify_queries(self, queries, authenticated_identity):
        return queries

    def filter_results(self, catalog, authenticated_identity):
        return type(catalog)(
            metadatastore_db=catalog.metadatastore_db,
            asset_registry_db=catalog.asset_registry_db,
            metadata=catalog.metadata,
            access_policy=catalog.access_policy,
            authenticated_identity=authenticated_identity,
        )


class SimpleAccessPolicy:
    """
    Refer to a mapping of user names to lists of entries they can access.

    >>> SimpleAccessPolicy({"alice": ["A", "B"], "bob": ["B"]})
    """

    ALL = object()  # sentinel

    def __init__(self, access_lists):
        self.access_lists = access_lists

    def check_compatibility(self, catalog):
        # This only works on in-memory Catalog or subclases.
        return isinstance(catalog, Catalog)

    def modify_queries(self, queries, authenticated_identity):
        allowed = self.access_lists.get(authenticated_identity, [])
        if (authenticated_identity is SpecialUsers.admin) or (allowed is self.ALL):
            modified_queries = queries
        else:
            modified_queries = list(queries)
            modified_queries.append(RawMongo(start={"uid": {"$in": allowed}}))
        return modified_queries

    def filter_results(self, catalog, authenticated_identity):
        return type(catalog)(
            metadatastore_db=catalog.metadatastore_db,
            asset_registry_db=catalog.asset_registry_db,
            metadata=catalog.metadata,
            access_policy=catalog.access_policy,
            authenticated_identity=authenticated_identity,
        )


def _get_database(uri):
    if not pymongo.uri_parser.parse_uri(uri)["database"]:
        raise ValueError(
            f"Invalid URI: {uri!r} " f"Did you forget to include a database?"
        )
    else:
        client = pymongo.MongoClient(uri)
        return client.get_database()
