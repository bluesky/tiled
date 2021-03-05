import json

from fastapi import APIRouter, Depends, HTTPException
from starlette.responses import StreamingResponse
from catalog_server.core import datasource

from .client import BlueskyRun


router = APIRouter()


@router.get("/documents")
def documents(datasource=Depends(datasource)):
    if not isinstance(datasource, BlueskyRun):
        raise HTTPException(status_code=404)

    def generator():
        "document stream as newline-delimited JSON"
        for item in datasource.documents():
            yield (json.dumps(item) + "\n")

    return StreamingResponse(generator, media_type="application/x-ndjson")
