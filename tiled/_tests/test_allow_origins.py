from tiled.client import from_config

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


def test_cors_enforcement():
    with from_config(strict_config) as client:
        request = client.context.http_client.build_request(
            "OPTIONS",
            "/",
            headers={
                "Access-Control-Request-Method": "GET",
                "Origin": "https://example.com",
            },
        )
        response = client.context.http_client.send(request)
        assert response.status_code == 400


def test_allow_origins():
    with from_config(permissive_config) as client:
        request = client.context.http_client.build_request(
            "OPTIONS",
            "/",
            headers={
                "Access-Control-Request-Method": "GET",
                "Origin": "https://example.com",
            },
        )
        response = client.context.http_client.send(request)
        assert response.status_code == 200
