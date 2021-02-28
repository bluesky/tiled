import abc
from functools import lru_cache
import importlib
import json
import operator
import os

from pydantic import BaseSettings, validator

_DEMO_DEFAULT_ROOT_CATALOG = "catalog_server.example_catalogs:hdf5_catalog"


class Settings(BaseSettings):

    catalog_object_path: str = os.getenv("ROOT_CATALOG", _DEMO_DEFAULT_ROOT_CATALOG)
    # dask_scheduler_address : str = os.getenv("DASK_SCHEDULER")

    @validator("catalog_object_path")
    def valid_object_path(cls, value):
        # TODO This could be more precise to catch more error cases.
        import_path, obj_path = str(value).split(":")
        for token in import_path.split("."):
            if not token.isidentifier():
                raise ValueError("Not a valid import path")
        for token in obj_path.split("."):
            if not token.isidentifier():
                raise ValueError("Not a valid attribute in a module")
        return str(value)

    @property
    def catalog(self):
        import_path, obj_path = self.catalog_object_path.split(":")
        module = importlib.import_module(import_path)
        return operator.attrgetter(obj_path)(module)


@lru_cache()
def get_settings():
    return Settings()


# @lru_cache()
# def get_dask_client():
#     "Connect to a specified dask scheduler, or start a LocalCluster."
#     address = get_settings().dask_scheduler_address
#     if address:
#         # Connect to an existing cluster.
#         client = Client(address, asynchronous=True)
#     else:
#         # Start a distributed.LocalCluster.
#         client = Client(asynchronous=True, processes=False)
#     return client


def get_entry(path, current_user):
    root_catalog = get_settings().catalog
    catalog = root_catalog.authenticated_as(current_user)
    # Traverse into sub-catalog(s).
    for entry in (path or "").split("/"):
        if entry:
            catalog = catalog[entry]
    return catalog


def len_or_approx(catalog):
    try:
        return len(catalog)
    except TypeError:
        return operator.length_hint(catalog)


def get_chunk(chunk):
    "dask array -> numpy array"
    return chunk.compute(scheduler="threads")


def pagination_links(offset, limit, length_hint):
    # TODO Include root path in links.
    # root_path = request.scope.get("/")
    links = {
        "self": f"/?page[offset]={offset}&page[limit]={limit}",
        # These are conditionally overwritten below.
        "first": None,
        "last": None,
        "next": None,
        "prev": None,
    }
    if limit:
        last_page = length_hint // limit
        links.update(
            {
                "first": f"/?page[offset]={0}&page[limit]={limit}",
                "last": f"/?page[offset]={last_page}&page[limit]={limit}",
            }
        )
    if offset + limit < length_hint:
        links["next"] = f"/?page[offset]={offset + limit}&page[limit]={limit}"
    if offset > 0:
        links["prev"] = f"/?page[offset]={max(0, offset - limit)}&page[limit]={limit}"
    return links


class DuckCatalog(metaclass=abc.ABCMeta):
    """
    Used for isinstance(obj, DuckCatalog):
    """

    @classmethod
    def __subclasshook__(cls, candidate):
        # If the following condition is True, candidate is recognized
        # to "quack" like a Catalog.
        EXPECTED_ATTRS = (
            "__getitem__",
            "__iter__",
            "keys_indexer",
            "items_indexer",
        )
        return all(hasattr(candidate, attr) for attr in EXPECTED_ATTRS)


class ArraySerializationRegistry:
    def __init__(self):
        # Map MIME types to functions
        self._registry = {}

    @property
    def media_types(self):
        return self._registry.keys()

    def register_media_type(self, media_type, serializer):
        self._registry[media_type] = serializer

    def serialize(self, media_type, array):
        serializer = self._registry[media_type]
        return serializer(array)


array_serialization_registry = ArraySerializationRegistry()
array_serialization_registry.register_media_type("application/octet-stream", memoryview)
array_serialization_registry.register_media_type(
    "application/json", lambda array: json.dumps(array.tolist()).encode()
)


def serialize_array(media_type, array):
    return array_serialization_registry.serialize(media_type, array)


array_media_types = array_serialization_registry.media_types
