import re

import httpx
import pytest
from fastapi import APIRouter
from starlette.status import HTTP_404_NOT_FOUND, HTTP_500_INTERNAL_SERVER_ERROR

from ..client import Context, from_context
from ..client.utils import handle_error
from ..server.app import build_app_from_config

config = {
    "authentication": {"single_user_api_key": "secret"},
    "trees": [{"path": "/", "tree": "tiled.examples.generated_minimal:tree"}],
}
router = APIRouter()


@router.get("/error")
def error():
    1 / 0  # type: ignore error!


def total_request_time(client, code):
    metrics = client.context.http_client.get("/api/v1/metrics").read().splitlines()
    pattern = re.compile(
        rf'^tiled_request_duration_seconds_bucket{{code="{code}",.*}} (\d+.\d+)$'.encode()
    )
    request_duration_buckets = []
    for metric in metrics:
        match = pattern.match(metric)
        if match:
            request_duration_buckets.append(float(match.group(1)))
    return sum(request_duration_buckets)


def test_error_code():
    app = build_app_from_config(config)
    app.include_router(router)
    with Context.from_app(app, raise_server_exceptions=False) as context:
        client = from_context(context)
        baseline_time = {
            code: total_request_time(client, code) for code in (200, 404, 500)
        }
        list(client)
        assert total_request_time(client, 200) - baseline_time[200] > 0
        assert total_request_time(client, 500) - baseline_time[500] == 0
        client.context.http_client.raise_server_exceptions = False
        response_500 = client.context.http_client.get("/error")
        assert response_500.status_code == HTTP_500_INTERNAL_SERVER_ERROR
        assert total_request_time(client, 500) - baseline_time[500] > 0
        response_404 = client.context.http_client.get("/does_not_exist")
        assert response_404.status_code == HTTP_404_NOT_FOUND
        assert total_request_time(client, 404) - baseline_time[404] > 0


def test_correlation_id():
    "Test that exceptions include correlation ID."
    app = build_app_from_config(config)
    app.include_router(router)
    with Context.from_app(app, raise_server_exceptions=False) as context:
        client = from_context(context)
        response_500 = client.context.http_client.get("/error")
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            handle_error(response_500)
        assert "correlation ID" in exc_info.value.args[0]
