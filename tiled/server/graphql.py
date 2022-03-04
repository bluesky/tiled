import math
from typing import List, Optional

import strawberry
from fastapi import Depends, HTTPException, Security
from strawberry.fastapi import GraphQLRouter
from strawberry.scalars import JSON
from strawberry.types import Info
from strawberry.tools import create_type, merge_types

from . import schemas
from .authentication import get_current_principal
from .core import JSON_MIME_TYPE, construct_resource, len_or_approx
from .dependencies import entry, get_root_tree
from .utils import get_base_url


@strawberry.type
class StructureType:
    micro: Optional[JSON]
    macro: Optional[JSON]


@strawberry.type
class NodeAttributesType:
    structure_family: Optional[str]
    specs: Optional[List[str]]
    metadata: Optional[JSON]  # free-form, user-specified dict
    structure: Optional[StructureType]
    count: Optional[int]


@strawberry.type
class PaginationCursor:
    offset: int
    limit: int


@strawberry.type
class PaginationLinksType:
    self: Optional[PaginationCursor]
    next: Optional[PaginationCursor]
    prev: Optional[PaginationCursor]
    first: Optional[PaginationCursor]
    last: Optional[PaginationCursor]


@strawberry.type
class ErrorType:
    code: int
    message: str


@strawberry.type
class ResourceType:
    id: str
    attributes: NodeAttributesType
    links: Optional[JSON]
    meta: Optional[JSON]


@strawberry.type
class ResponseType:
    data: Optional[List[ResourceType]]
    error: Optional[ErrorType]
    links: Optional[PaginationLinksType]
    meta: Optional[JSON]


async def get_context(
    principal=Security(get_current_principal, scopes=["read:metadata"]),
    root_tree=Depends(get_root_tree),
):
    return {"principal": principal, "root_tree": root_tree}


def pagination_links(offset, limit, length_hint):
    links = {
        "self": PaginationCursor(offset=offset, limit=limit),
        "first": None,
        "last": None,
        "next": None,
        "prev": None,
    }
    if limit:
        last_page = math.floor(length_hint / limit) * limit
        links.update(
            {
                "first": PaginationCursor(offset=0, limit=limit),
                "last": PaginationCursor(offset=last_page, limit=limit),
            }
        )
    if offset + limit < length_hint:
        links["next"] = PaginationCursor(offset=offset + limit, limit=limit)
    if offset > 0:
        links["prev"] = PaginationCursor(offset=max(0, offset - limit), limit=limit)

    return PaginationLinksType(**links)

@strawberry.field
def hello(info: Info) -> str:
    return f"Hello {info.context['principal']}"


@strawberry.field
def search(path: str, offset: int, limit: int, info: Info) -> ResponseType:
    request = info.context["request"]
    principal = info.context["principal"]
    root_tree = info.context["root_tree"]
    try:
        # FIXME within fastapi this would be constructed via dependency
        # injection but not clear if this can be applied here
        tree = entry(path, request, principal, root_tree)

    except HTTPException:
        return ResponseType(
            data=None,
            error=ErrorType(code=404, message=f"no entry at {path}"),
            links=None,
            meta=None,
        )

    count = len_or_approx(tree)
    links = pagination_links(offset, limit, count)
    base_url = get_base_url(request)
    path_parts = [segment for segment in path.split("/") if segment]
    fields = list(schemas.EntryFields)
    select_metadata = None
    omit_links = False
    media_type = JSON_MIME_TYPE

    data = []

    items = tree.items_indexer[offset : offset + limit]  # noqa: E203

    for k, v in items:
        resource = construct_resource(
            base_url,
            path_parts + [k],
            v,
            fields,
            select_metadata,
            omit_links,
            media_type,
        )

        # FIXME unpacking/repacking like this is tedious
        # should probably just have a separate construct_resource function
        data.append(
            ResourceType(
                id=resource.id,
                attributes=NodeAttributesType(
                    structure_family=resource.attributes.structure_family,
                    specs=resource.attributes.specs,
                    metadata=resource.attributes.metadata,
                    structure=resource.attributes.structure,
                    count=resource.attributes.count,
                ),
                links=resource.links.dict(),
                meta=resource.meta,
            )
        )

    return ResponseType(data=data, links=links, meta={"count": count}, error=None)

BaseQuery = create_type("BaseQuery", [hello, search])

def get_router(queries):
    TiledQuery = merge_types("TiledQuery", tuple(queries))
    schema = strawberry.Schema(query=TiledQuery)
    router = GraphQLRouter(schema, context_getter=get_context)
    return router
