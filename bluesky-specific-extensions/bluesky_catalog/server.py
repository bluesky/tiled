import json
import msgpack

from fastapi import APIRouter, Depends, HTTPException, Request
import pydantic
from starlette.responses import StreamingResponse
from catalog_server.server.core import datasource


class NameDocumentPair(pydantic.BaseModel):
    name: str  # TODO Lock this down to an enum of the document types.
    document: dict


router = APIRouter()


@router.get("/documents/{path:path}", response_model=NameDocumentPair)
@router.get("/documents", response_model=NameDocumentPair, include_in_schema=False)
def documents(request: Request, datasource=Depends(datasource)):
    if not hasattr(datasource, "documents"):
        raise HTTPException(status_code=404)
    DEFAULT_MEDIA_TYPE = "application/json"
    media_types = request.headers.get("Accept", DEFAULT_MEDIA_TYPE).split(", ")
    for media_type in media_types:
        if media_type == "*/*":
            media_type = DEFAULT_MEDIA_TYPE
        if media_type == "application/x-msgpack":
            # (name, doc) pairs as msgpack
            # TODO: This has not yet been tested with a client. Does it work?
            # Do we need a msgpack.Packer properly mark the boundaries?
            generator = (msgpack.packb(item) for item in datasource.documents())
            return StreamingResponse(generator, media_type="application/x-msgpack")
        if media_type == "application/json":
            # (name, doc) pairs as newline-delimited JSON
            generator = (json.dumps(item) + "\n" for item in datasource.documents())
            return StreamingResponse(generator, media_type="application/x-ndjson")
    else:
        raise HTTPException(
            status_code=406,
            detail=", ".join(["application/json", "application/x-msgpack"]),
        )
