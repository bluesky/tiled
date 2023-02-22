import re

from fastapi import APIRouter

from ..client.constructors import from_app
from ..config import construct_build_app_kwargs
from ..server.app import build_app

config = {
    "authentication": {"single_user_api_key": "secret"},
    "trees": [{"path": "/", "tree": "tiled.examples.generated_minimal:tree"}],
}
build_app_kwargs = construct_build_app_kwargs(config)

router = APIRouter()


@router.get("/error")
def error():
    1 / 0  # error!


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
    app = build_app(**build_app_kwargs)
    app.include_router(router)

    client = from_app(app, raise_server_exceptions=False)
    list(client)
    assert total_request_time(client, 200) > 0
    assert total_request_time(client, 500) == 0
    client.context.http_client.raise_server_exceptions = False
    response_500 = client.context.http_client.get("/error")
    assert response_500.status_code == 500
    assert total_request_time(client, 500) > 0
    response_404 = client.context.http_client.get("/does_not_exist")
    assert response_404.status_code == 404
    assert total_request_time(client, 404) > 0
