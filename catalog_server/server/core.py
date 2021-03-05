import abc
from collections import defaultdict
import dataclasses
from functools import lru_cache
import importlib
import math
import operator
import os
import re
from typing import Any

import dask.base
from fastapi import Response
from pydantic import BaseSettings, validator

from . import models
from .. import queries  # This is not used, but it registers queries on import.
from ..query_registration import name_to_query_type
from ..media_type_registration import serialization_registry

del queries


_DEMO_DEFAULT_ROOT_CATALOG = (
    "catalog_server.examples.generic:nested_with_access_control"
)
_FILTER_PARAM_PATTERN = re.compile(r"filter___(?P<name>.*)___(?P<field>[^\d\W][\w\d]+)")


class Settings(BaseSettings):

    catalog_object_path: str = os.getenv("ROOT_CATALOG", _DEMO_DEFAULT_ROOT_CATALOG)
    allow_anonymous_access: bool = bool(int(os.getenv("ALLOW_ANONYMOUS_ACCESS", True)))
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


def pagination_links(route, path, offset, limit, length_hint):
    # TODO Include root path in links.
    # root_path = request.scope.get("/")
    links = {
        "self": f"{route}{path}?page[offset]={offset}&page[limit]={limit}",
        # These are conditionally overwritten below.
        "first": None,
        "last": None,
        "next": None,
        "prev": None,
    }
    if limit:
        last_page = math.floor(length_hint / limit) * limit
        links.update(
            {
                "first": f"{route}{path}?page[offset]={0}&page[limit]={limit}",
                "last": f"{route}{path}?page[offset]={last_page}&page[limit]={limit}",
            }
        )
    if offset + limit < length_hint:
        links[
            "next"
        ] = f"{route}{path}?page[offset]={offset + limit}&page[limit]={limit}"
    if offset > 0:
        links[
            "prev"
        ] = f"{route}{path}?page[offset]={max(0, offset - limit)}&page[limit]={limit}"
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
        )
        return all(hasattr(candidate, attr) for attr in EXPECTED_ATTRS)


def construct_entries_response(
    route,
    path,
    offset,
    limit,
    fields,
    filters,
    current_user,
):
    path = path.rstrip("/")
    if not path.startswith("/"):
        path = f"/{path}"
    try:
        catalog = get_entry(path, current_user)
    except KeyError:
        raise NoEntry(path)
    if not isinstance(catalog, DuckCatalog):
        raise WrongTypeForRoute("This is a Data Source, not a Catalog.")
    queries = defaultdict(
        dict
    )  # e.g. {"text": {"text": "dog"}, "lookup": {"key": "..."}}
    # Group the parameters by query type.
    for key, value in filters.items():
        if value is None:
            continue
        name, field = _FILTER_PARAM_PATTERN.match(key).groups()
        queries[name][field] = value
    # Apply the queries and obtain a narrowed catalog.
    for name, parameters in queries.items():
        query_class = name_to_query_type[name]
        query = query_class(**parameters)
        catalog = catalog.search(query)
    count = len_or_approx(catalog)
    links = pagination_links(route, path, offset, limit, count)
    data = []
    if fields:
        # Pull a page of items into memory.
        items = catalog.items_indexer[offset : offset + limit]
    else:
        # Pull a page of just the keys, which is cheaper.
        items = ((key, None) for key in catalog.keys_indexer[offset : offset + limit])
    for key, entry in items:
        resource = construct_resource(key, entry, fields)
        data.append(resource)
    return models.Response(data=data, links=links, meta={"count": count})


def construct_array_response(array, request_headers):
    DEFAULT_MEDIA_TYPE = "application/octet-stream"
    etag = dask.base.tokenize(array)
    if request_headers.get("If-None-Match", "") == etag:
        return Response(status_code=304)
    media_types = request_headers.get("Accept", DEFAULT_MEDIA_TYPE).split(", ")
    for media_type in media_types:
        if media_type == "*/*":
            media_type = DEFAULT_MEDIA_TYPE
        if media_type in serialization_registry.media_types("array"):
            content = serialization_registry("array", media_type, array)
            return PatchedResponse(
                content=content, media_type=media_type, headers={"ETag": etag}
            )
    else:
        raise UnsupportedMediaTypes(
            "None of the media types requested by the client are supported.",
            unsupported=media_types,
            supported=serialization_registry.media_types("array"),
        )


def construct_resource(key, entry, fields):
    attributes = {}
    if models.EntryFields.metadata in fields:
        attributes["metadata"] = entry.metadata
    if models.EntryFields.client_type_hint in fields:
        attributes["client_type_hint"] = getattr(entry, "client_type_hint", None)
    if isinstance(entry, DuckCatalog):
        if models.EntryFields.count in fields:
            attributes["count"] = len_or_approx(entry)
        resource = models.CatalogResource(
            **{
                "id": key,
                "attributes": models.CatalogAttributes(**attributes),
                "type": models.EntryType.catalog,
            }
        )
    else:
        if models.EntryFields.container in fields:
            attributes["container"] = entry.container
        if models.EntryFields.structure in fields:
            attributes["structure"] = dataclasses.asdict(entry.describe())
        resource = models.DataSourceResource(
            **{
                "id": key,
                "attributes": models.DataSourceAttributes(**attributes),
                "type": models.EntryType.datasource,
            }
        )
    return resource


class PatchedResponse(Response):
    "Patch the render method to accept memoryview."

    def render(self, content: Any) -> bytes:
        if isinstance(content, memoryview):
            return content.cast("B")
        return super().render(content)


class UnsupportedMediaTypes(Exception):
    pass


class NoEntry(KeyError):
    pass


class WrongTypeForRoute(Exception):
    pass
