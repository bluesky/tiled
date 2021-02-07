import abc
from functools import lru_cache
import json
from operator import length_hint
import tempfile

import dask
from dask.distributed import Client
from fastapi import HTTPException
from pydantic import BaseSettings


class Settings(BaseSettings):
    from example_catalogs import catalog

    catalog = catalog


@lru_cache()
def get_settings():
    return Settings()


@lru_cache()
def get_dask_client():
    "Start a dask cluster than uses threaded workers, and return its Client."
    # For now avoid placing dask-worker-space in cwd (the default) because
    # triggers server reloads in uvicorn --reload mode. We will want this to be
    # configurable in the future.
    temp_dask_worker_space = tempfile.TemporaryDirectory().name
    DASK_CONFIG = {"temporary-directory": temp_dask_worker_space}
    dask.config.update(dask.config.config, DASK_CONFIG, priority="new")

    return Client(asynchronous=True, processes=False)


def get_entry(path):
    catalog = get_settings().catalog
    # Traverse into sub-catalog(s).
    for entry in (path or "").split("/"):
        if entry:
            catalog = catalog[entry]
    return catalog


async def get_chunk(chunk):
    "dask array -> numpy array"
    # Make dask pull the dask into memory using its threaded workers.
    return await get_dask_client().compute(chunk)


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
        links[
            "prev"
        ] = f"/?page[offset]={max(0, offset - limit)}&page[limit]={limit + 1}"
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
            "index",
        )
        return all(hasattr(candidate, attr) for attr in EXPECTED_ATTRS)


def construct_catalogs_entries_response(
    path, offset, limit, query, include_metadata, include_description
):
    path = path.rstrip("/")
    try:
        catalog = get_entry(path)
    except KeyError:
        raise HTTPException(status_code=404, detail="No such entry.")
    if not isinstance(catalog, DuckCatalog):
        raise HTTPException(
            status_code=404, detail="This is a Data Source, not a Catalog."
        )
    if query is not None:
        catalog = catalog.search(query)
    approx_len = length_hint(catalog)
    links = pagination_links(offset, limit, approx_len)
    data = []
    if include_metadata or include_description:
        # Pull a page of items into memory.
        items = catalog.items_indexer[offset : offset + limit]
    else:
        # Pull a page of just the keys, which is cheaper.
        items = ((key, None) for key in catalog.keys_indexer[offset : offset + limit])
    for key, entry in items:
        obj = {
            "id": f"{path}/{key}",
            "attributes": {
                "key": key,
            },
            "meta": {
                "__module__": getattr(type(entry), "__module__"),
                "__qualname__": getattr(type(entry), "__qualname__"),
            },
            "links": {},
        }
        if include_metadata:
            obj["attributes"]["metadata"] = entry.metadata
        if isinstance(entry, DuckCatalog):
            obj["type"] = "catalog"
            obj["links"]["keys"] = f"/catalogs/keys/{path}/{key}"
            obj["links"]["entries"] = f"/catalogs/entries/{path}/{key}"
            obj["links"]["description"] = f"/catalogs/description/{path}/{key}"
            data.append(obj)
        else:
            obj["type"] = "datasource"
            if include_description:
                obj["attributes"]["description"] = entry.describe()
            else:
                obj["links"]["description"] = f"/datasource/description/{path}/{key}"
            data.append(obj)
    response = {
        "data": data,
        "links": links,
        "meta": {"count": approx_len},
    }
    return response


def construct_datasource_response(path, key, include_metadata, include_description):
    path = path.rstrip("/")
    try:
        datasource = get_entry(path)
    except KeyError:
        raise HTTPException(status_code=404, detail="No such entry.")
    data = {
        "id": f"{path}/{key}",
        "attributes": {
            "key": key,
            "description": datasource.describe(),
        },
        "meta": {
            "__module__": getattr(type(datasource), "__module__"),
            "__qualname__": getattr(type(datasource), "__qualname__"),
        },
        "links": {},
    }
    response = {
        "data": data,
        "links": {},
    }
    return response


class SerializationRegistry:
    def __init__(self):
        # Map MIME types to functions
        self._registry = {}
        self._dask_client = get_dask_client()

    def register_media_type(self, media_type, serializer):
        self._registry[media_type] = serializer

    async def serialize(self, media_type, array):
        serializer = self._registry[media_type]
        return await self._dask_client.submit(serializer, array)


serialization_registry = SerializationRegistry()
serialization_registry.register_media_type(
    "application/octet-stream", lambda array: array.tobytes()
)
serialization_registry.register_media_type(
    "application/json", lambda array: json.dumps(array.tolist()).encode()
)


def serialize_array(media_type, array):
    return serialization_registry.serialize(media_type, array)
