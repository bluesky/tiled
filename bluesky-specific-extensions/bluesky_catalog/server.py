import json

from fastapi import APIRouter, Depends, HTTPException
import pydantic
from starlette.responses import StreamingResponse
from catalog_server.server.core import datasource


class NameDocumentPair(pydantic.BaseModel):
    name: str  # TODO Lock this down to an enum of the document types.
    document: dict


router = APIRouter()


@router.get("/documents/{path:path}", response_model=NameDocumentPair)
@router.get("/documents", response_model=NameDocumentPair, include_in_schema=False)
def documents(datasource=Depends(datasource)):
    if not hasattr(datasource, "documents"):
        raise HTTPException(status_code=404)
    # (name, doc) pairs as newline-delimited JSON
    generator = (json.dumps(item) + "\n" for item in datasource.documents())
    return StreamingResponse(generator, media_type="application/x-ndjson")
