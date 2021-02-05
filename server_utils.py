import abc
import json
from functools import lru_cache
import tempfile

import dask
from dask.distributed import Client
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
    last_page = length_hint // limit
    # TODO Include root path in links.
    # root_path = request.scope.get("/")
    links = {
        "self": f"/?page[offset]={offset}&page[limit]={limit}",
        "first": f"/?page[offset]={0}&page[limit]={limit}",
        "last": f"/?page[offset]={last_page}&page[limit]={limit}",
    }
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


def construct_response_data_from_items(path, items, describe):
    data = {}
    catalogs, datasources = [], []
    for key, entry in items:
        obj = {
            "key": key,
            "metadata": entry.metadata,
            "__module__": getattr(type(entry), "__module__"),
            "__qualname__": getattr(type(entry), "__qualname__"),
            "links": {},
        }
        if isinstance(entry, DuckCatalog):
            obj["links"]["list"] = f"/catalogs/list/{path}/{key}"
            obj["links"]["entries"] = f"/catalogs/list/{path}/{key}"
            catalogs.append(obj)
        else:
            if describe:
                obj["description"] = entry.describe()
            else:
                obj["links"]["describe"] = f"/catalogs/list/{path}/{key}"
            datasources.append(obj)
    data["catalogs"] = catalogs
    data["datasources"] = datasources
    return data


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
