import collections.abc
from dataclasses import dataclass
import importlib
import itertools
import functools
import warnings

import entrypoints
import event_model
import dask
import dask.array
import numpy
import pymongo
import xarray

from catalog_server.containers.array import ArrayStructure, MachineDataType
from catalog_server.containers.xarray import (
    DataArrayStructure,
    DatasetStructure,
    VariableStructure,
)
from catalog_server.query_registration import QueryTranslationRegistry, register
from catalog_server.queries import FullText, KeyLookup
from catalog_server.utils import (
    authenticated,
    catalog_repr,
    DictView,
    IndexCallable,
    slice_to_interval,
    SpecialUsers,
    UNCHANGED,
)
from catalog_server.catalogs.in_memory import Catalog as CatalogInMemory
from catalog_server.utils import LazyMap


class BlueskyRun(CatalogInMemory):
    client_type_hint = "BlueskyRun"

    def __init__(self, *args, filler, **kwargs):
        super().__init__(*args, **kwargs)
        self.filler = filler

    def new_variation(self, *args, filler=UNCHANGED, **kwargs):
        if filler is UNCHANGED:
            filler = self.filler
        return super().new_variation(*args, filler=filler, **kwargs)

    def __repr__(self):
        return f"<{type(self).__name__}(uid={self.metadata['start']['uid']})>"

    def documents(self):
        yield ("start", self.metadata["start"])
        stop_doc = self.metadata["stop"]
        # TODO: All the other documents...
        if stop_doc is not None:
            yield ("stop", stop_doc)


class BlueskyEventStream(CatalogInMemory):
    client_type_hint = "BlueskyEventStream"

    @property
    def descriptors(self):
        return self.metadata["descriptors"]


class DatasetFromDocuments:
    """
    An xarray.Dataset from a sub-dict of an Event stream
    """

    container = "dataset"

    def __init__(
        self,
        *,
        cutoff_seq_num,
        event_descriptors,
        event_collection,
        datum_collection,
        resource_collection,
        sub_dict,
        metadata=None,
    ):
        self._metadata = metadata or {}
        self._cutoff_seq_num = cutoff_seq_num
        self._event_descriptors = event_descriptors
        self._event_collection = event_collection
        self._datum_collection = datum_collection
        self._resource_collection = resource_collection
        self._sub_dict = sub_dict
        self._events = []

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def describe(self):
        # The `data_keys` in a series of Event Descriptor documents with the same
        # `name` MUST be alike, so we can choose one arbitrarily.
        descriptor, *_ = self._event_descriptors
        # Unfortunately, the Event Descriptor does not contain all the
        # information we need, so we have to pull one Event at this point
        query = {"descriptor": descriptor["uid"]}
        sample_event = self._event_collection.find_one(query)
        data_vars = {}
        dim_counter = itertools.count()
        for key, field_metadata in descriptor["data_keys"].items():
            # if the EventDescriptor doesn't provide names for the
            # dimensions (it's optional) use the same default dimension
            # names that xarray would.
            try:
                dims = ["time"] + field_metadata["dims"]
            except KeyError:
                ndim = min(1, len(field_metadata["shape"]))
                dims = ["time"] + [f"dim_{next(dim_counter)}" for _ in range(ndim)]
            attrs = {}
            # Record which object (i.e. device) this column is associated with,
            # which enables one to find the relevant configuration, if any.
            for object_name, keys_ in descriptor.get("object_keys", {}).items():
                for item in keys_:
                    if item == key:
                        attrs["object"] = object_name
                        break
            units = field_metadata.get("units")
            if units:
                if isinstance(units, str):
                    attrs["units_string"] = units
                # TODO We may soon add a more structured units type, which
                # would likely be a dict here.
            shape = tuple((self._cutoff_seq_num, *field_metadata["shape"]))
            numpy_dtype = numpy.dtype(type(sample_event[self._sub_dict][key]))
            data = ArrayStructure(
                shape=shape,
                dtype=MachineDataType.from_numpy_dtype(numpy_dtype),
                chunks=tuple((s,) for s in shape),  # TODO subdivide
            )
            variable = VariableStructure(dims=dims, data=data, attrs=attrs)
            data_array = DataArrayStructure(variable, coords={}, name=key)
            data_vars[key] = data_array
        return DatasetStructure(data_vars=data_vars, coords={}, attrs={})

    def read(self):
        structure = self.describe()
        data_arrays = {}
        for key, data_array in structure.data_vars.items():
            variable = data_array.variable
            # TODO Handle chunks.
            for dim in variable.data.chunks:
                if len(dim) > 1:
                    raise NotImplementedError
            dask_array = dask.array.from_delayed(
                dask.delayed(self._get_column)(key, block=(0,)),
                shape=variable.data.shape,
                dtype=variable.data.dtype.to_numpy_dtype(),
            )
            data_array = xarray.DataArray(dask_array, attrs=variable.attrs)
            data_arrays[key] = data_array
        return xarray.Dataset(data_arrays)

    def _get_column(self, key, block):
        if block != (0,):
            raise NotImplementedError
            # TODO Implement columns that are internally chunked.
        self._load_data()
        col = numpy.array(_transpose(self._events, [key], self._sub_dict)[key])
        print(col)
        return col

    def _load_data(self):
        # TODO Support partial loading.
        if self._events:
            return
        for descriptor in sorted(self._event_descriptors, key=lambda d: d["time"]):
            # TODO Sort, and deal with repeated seq_num.
            # May $last would be useful for that, or we could just drop
            # duplicate on the Python side since they are rare.
            query = {"descriptor": descriptor["uid"]}
            cursor = self._event_collection.find(query, {"_id": False})
            self._events.extend(cursor)
        print(self._events)


class Catalog(collections.abc.Mapping):

    # Define classmethods for managing what queries this Catalog knows.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    @classmethod
    def from_uri(
        cls,
        metadatastore,
        *,
        asset_registry=None,
        handler_registry=None,
        root_map=None,
        transforms=None,
        filler_class=event_model.Filler,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
    ):
        """
        Create a Catalog from MongoDB with the "normalized" (original) layout.

        Parameters
        ----------
        handler_registry: dict, optional
            This is passed to the Filler or whatever class is given in the
            filler_class parameter below.

            Maps each 'spec' (a string identifying a given type or external
            resource) to a handler class.

            A 'handler class' may be any callable with the signature::

                handler_class(resource_path, root, **resource_kwargs)

            It is expected to return an object, a 'handler instance', which is also
            callable and has the following signature::

                handler_instance(**datum_kwargs)

            As the names 'handler class' and 'handler instance' suggest, this is
            typically implemented using a class that implements ``__init__`` and
            ``__call__``, with the respective signatures. But in general it may be
            any callable-that-returns-a-callable.
        root_map: dict, optional
            This is passed to Filler or whatever class is given in the filler_class
            parameter below.

            str -> str mapping to account for temporarily moved/copied/remounted
            files.  Any resources which have a ``root`` in ``root_map`` will be
            loaded using the mapped ``root``.
        filler_class: type
            This is Filler by default. It can be a Filler subclass,
            ``functools.partial(Filler, ...)``, or any class that provides the same
            methods as ``DocumentRouter``.
        transforms: dict
            A dict that maps any subset of the keys {start, stop, resource, descriptor}
            to a function that accepts a document of the corresponding type and
            returns it, potentially modified. This feature is for patching up
            erroneous metadata. It is intended for quick, temporary fixes that
            may later be applied permanently to the data at rest
            (e.g., via a database migration).
        """
        metadatastore_db = _get_database(metadatastore)
        if asset_registry is None:
            asset_registry_db = metadatastore_db
        else:
            asset_registry_db = _get_database(asset_registry)
        if isinstance(filler_class, str):
            module_name, _, class_name = filler_class.rpartition(".")
            filler_class = getattr(importlib.import_module(module_name), class_name)
        root_map = root_map or {}
        transforms = parse_transforms(transforms)
        if handler_registry is None:
            handler_registry = discover_handlers()
        handler_registry = parse_handler_registry(handler_registry)
        return cls(
            metadatastore_db=metadatastore_db,
            asset_registry_db=asset_registry_db,
            filler_class=filler_class,
            handler_registry=handler_registry,
            root_map=root_map,
            transforms=transforms,
            metadata=metadata,
            access_policy=access_policy,
            authenticated_identity=authenticated_identity,
        )

    def __init__(
        self,
        metadatastore_db,
        asset_registry_db,
        filler_class,
        handler_registry,
        root_map,
        transforms,
        metadata=None,
        queries=None,
        access_policy=None,
        authenticated_identity=None,
    ):
        "This is not user-facing. Use Catalog.from_uri."
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

        self.filler_class = filler_class
        self._handler_registry = handler_registry
        self.handler_registry = event_model.HandlerRegistryView(self._handler_registry)
        self.root_map = root_map
        self.transforms = transforms
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

    def register_handler(self, spec, handler, overwrite=False):
        if (not overwrite) and (spec in self._handler_registry):
            original = self._handler_registry[spec]
            if original is handler:
                return
            raise event_model.DuplicateHandler(
                f"There is already a handler registered for the spec {spec!r}. "
                f"Use overwrite=True to deregister the original.\n"
                f"Original: {original}\n"
                f"New: {handler}"
            )
        self._handler_registry[spec] = handler

    def deregister_handler(self, spec):
        self._handler_registry.pop(spec, None)

    def new_variation(
        self,
        *args,
        metadata=UNCHANGED,
        queries=UNCHANGED,
        authenticated_identity=UNCHANGED,
        **kwargs,
    ):
        if metadata is UNCHANGED:
            metadata = self._metadata
        if queries is UNCHANGED:
            queries = self._high_level_queries
        if authenticated_identity is UNCHANGED:
            authenticated_identity = self._authenticated_identity
        return type(self)(
            *args,
            metadatastore_db=self._metadatastore_db,
            asset_registry_db=self._asset_registry_db,
            filler_class=self.filler_class,
            handler_registry=self.handler_registry,
            root_map=self.root_map,
            transforms=self.transforms,
            queries=queries,
            access_policy=self.access_policy,
            authenticated_identity=authenticated_identity,
            **kwargs,
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

    def _build_run(self, run_start_doc):
        uid = run_start_doc["uid"]
        # This may be None; that's fine.
        run_stop_doc = self._get_stop_doc(uid)
        stream_names = self._event_descriptor_collection.distinct(
            "name",
            {"run_start": uid},
        )
        mapping = {}
        for stream_name in stream_names:
            mapping[stream_name] = functools.partial(
                self._build_event_stream,
                run_start_uid=uid,
                stream_name=stream_name,
                is_complete=(run_stop_doc is not None),
            )
        filler = self.filler_class(
            handler_registry=self.handler_registry,
            root_map=self.root_map,
            inplace=False,
        )
        return BlueskyRun(
            LazyMap(mapping),
            metadata={"start": run_start_doc, "stop": run_stop_doc},
            filler=filler,
        )

    def _build_event_stream(self, *, run_start_uid, stream_name, is_complete):
        event_descriptors = list(
            self._event_descriptor_collection.find(
                {"run_start": run_start_uid, "name": stream_name}, {"_id": False}
            )
        )
        event_descriptor_uids = [doc["uid"] for doc in event_descriptors]
        # We need each of the sub-dicts to have a consistent length. If
        # Events are still being added, we need to choose a consistent
        # cutoff. If not, we need to know the length anyway. Note that this
        # is not the same thing as the number of Event documents in the
        # stream because seq_num may be repeated, nonunique.
        (result,) = list(
            self._event_collection.aggregate(
                [
                    {"$match": {"descriptor": {"$in": event_descriptor_uids}}},
                    {
                        "$group": {
                            "_id": "descriptor",
                            "highest_seq_num": {"$max": "$seq_num"},
                        },
                    },
                ]
            )
        )
        cutoff_seq_num = result["highest_seq_num"]
        mapping = LazyMap(
            {
                "data": lambda: DatasetFromDocuments(
                    cutoff_seq_num=cutoff_seq_num,
                    event_descriptors=event_descriptors,
                    event_collection=self._event_collection,
                    datum_collection=self._datum_collection,
                    resource_collection=self._resource_collection,
                    sub_dict="data",
                ),
                # TODO timestamps, config, config_timestamps
            }
        )

        metadata = {"descriptors": event_descriptors, "stream_name": stream_name}
        return BlueskyEventStream(mapping, metadata=metadata)

    @authenticated
    def __getitem__(self, key):
        # Lookup this key *within the search results* of this Catalog.
        query = self._mongo_query({"uid": key})
        run_start_doc = self._run_start_collection.find_one(query, {"_id": False})
        if run_start_doc is None:
            raise KeyError(key)
        return self._build_run(run_start_doc)

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
            catalog = self.new_variation()
        return catalog

    @authenticated
    def search(self, query):
        """
        Return a Catalog with a subset of the mapping.
        """
        return self.new_variation(
            queries=self._high_level_queries + (query,),
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
            yield (run_start_doc["uid"], self._build_run(run_start_doc))

    def _item_by_index(self, index):
        if index >= len(self):
            raise IndexError(f"index {index} out of range for length {len(self)}")
        run_start_doc = next(
            self._chunked_find(
                self._run_start_collection, self._mongo_query(), skip=index, limit=1
            )
        )
        key = run_start_doc["uid"]
        value = self._build_run(run_start_doc)
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
        return catalog.new_variation(authenticated_identity=authenticated_identity)


def _get_database(uri):
    if not pymongo.uri_parser.parse_uri(uri)["database"]:
        raise ValueError(
            f"Invalid URI: {uri!r} " f"Did you forget to include a database?"
        )
    else:
        client = pymongo.MongoClient(uri)
        return client.get_database()


def discover_handlers(entrypoint_group_name="databroker.handlers", skip_failures=True):
    """
    Discover handlers via entrypoints.

    Parameters
    ----------
    entrypoint_group_name: str
        Default is 'databroker.handlers', the "official" databroker entrypoint
        for handlers.
    skip_failures: boolean
        True by default. Errors loading a handler class are converted to
        warnings if this is True.

    Returns
    -------
    handler_registry: dict
        A suitable default handler registry
    """
    group = entrypoints.get_group_named(entrypoint_group_name)
    group_all = entrypoints.get_group_all(entrypoint_group_name)
    if len(group_all) != len(group):
        # There are some name collisions. Let's go digging for them.
        for name, matches in itertools.groupby(group_all, lambda ep: ep.name):
            matches = list(matches)
            if len(matches) != 1:
                winner = group[name]
                warnings.warn(
                    f"There are {len(matches)} entrypoints for the "
                    f"databroker handler spec {name!r}. "
                    f"They are {matches}. The match {winner} has won the race."
                )
    handler_registry = {}
    for name, entrypoint in group.items():
        try:
            handler_class = entrypoint.load()
        except Exception as exc:
            if skip_failures:
                warnings.warn(
                    f"Skipping {entrypoint!r} which failed to load. "
                    f"Exception: {exc!r}"
                )
                continue
            else:
                raise
        handler_registry[name] = handler_class

    return handler_registry


def parse_handler_registry(handler_registry):
    """
    Parse mapping of spec name to 'import path' into mapping to class itself.

    Parameters
    ----------
    handler_registry : dict
        Values may be string 'import paths' to classes or actual classes.

    Examples
    --------
    Pass in name; get back actual class.

    >>> parse_handler_registry({'my_spec': 'package.module.ClassName'})
    {'my_spec': <package.module.ClassName>}

    """
    result = {}
    for spec, handler_str in handler_registry.items():
        if isinstance(handler_str, str):
            module_name, _, class_name = handler_str.rpartition(".")
            class_ = getattr(importlib.import_module(module_name), class_name)
        else:
            class_ = handler_str
        result[spec] = class_
    return result


def parse_transforms(transforms):
    """
    Parse mapping of spec name to 'import path' into mapping to class itself.

    Parameters
    ----------
    transforms : collections.abc.Mapping or None
        A collections.abc.Mapping or subclass, that maps any subset of the
        keys {start, stop, resource, descriptor} to a function (or a string
        import path) that accepts a document of the corresponding type and
        returns it, potentially modified. This feature is for patching up
        erroneous metadata. It is intended for quick, temporary fixes that
        may later be applied permanently to the data at rest (e.g via a
        database migration).

    Examples
    --------
    Pass in name; get back actual class.

    >>> parse_transforms({'descriptor': 'package.module.function_name'})
    {'descriptor': <package.module.function_name>}

    """
    transformable = {"start", "stop", "resource", "descriptor"}

    if transforms is None:
        result = {key: _no_op for key in transformable}
        return result
    elif isinstance(transforms, collections.abc.Mapping):
        if len(transforms.keys() - transformable) > 0:
            raise NotImplementedError(
                f"Transforms for {transforms.keys() - transformable} "
                f"are not supported."
            )
        result = {}

        for name in transformable:
            transform = transforms.get(name)
            if isinstance(transform, str):
                module_name, _, class_name = transform.rpartition(".")
                function = getattr(importlib.import_module(module_name), class_name)
            elif transform is None:
                function = _no_op
            else:
                function = transform
            result[name] = function
        return result
    else:
        raise ValueError(
            f"Invalid transforms argument {transforms}. "
            f"transforms must be None or a dictionary."
        )


def _no_op(doc):
    return doc


def _transpose(in_data, keys, field):
    """Turn a list of dicts into dict of lists

    Parameters
    ----------
    in_data : list
        A list of dicts which contain at least one dict.
        All of the inner dicts must have at least the keys
        in `keys`

    keys : list
        The list of keys to extract

    field : str
        The field in the outer dict to use

    Returns
    -------
    transpose : dict
        The transpose of the data
    """
    out = {k: [None] * len(in_data) for k in keys}
    for j, ev in enumerate(in_data):
        dd = ev[field]
        for k in keys:
            out[k][j] = dd[k]
    return out
