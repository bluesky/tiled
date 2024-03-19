from starlette.status import HTTP_200_OK, HTTP_400_BAD_REQUEST

from ..client import Context
from ..server.app import build_app_from_config

strict_config = {
    "authentication": {
        "allow_anonymous_access": True,
    },
    "trees": [
        {
            "tree": "tiled.examples.generated_minimal:tree",
            "path": "/",
        },
    ],
}

permissive_config = strict_config.copy()
permissive_config["allow_origins"] = ["https://example.com"]
headers = {
    "Access-Control-Request-Method": "GET",
    "Origin": "https://example.com",
}


def test_cors_enforcement():
    with Context.from_app(build_app_from_config(strict_config)) as context:
        request = context.http_client.build_request("OPTIONS", "/", headers=headers)
        response = context.http_client.send(request)
        assert response.status_code == HTTP_400_BAD_REQUEST


def test_allow_origins():
    with Context.from_app(build_app_from_config(permissive_config)) as context:
        request = context.http_client.build_request("OPTIONS", "/", headers=headers)
        response = context.http_client.send(request)
        assert response.status_code == HTTP_200_OK
