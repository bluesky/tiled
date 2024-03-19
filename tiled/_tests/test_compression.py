import numpy
import pytest
from starlette.status import HTTP_200_OK
from starlette.testclient import TestClient

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..server.app import build_app


@pytest.fixture
def app():
    metadata = {str(i): {str(j): j for j in range(100)} for i in range(100)}
    tree = MapAdapter(
        {
            "compresses_well": ArrayAdapter.from_array(
                numpy.zeros((1000, 1000)), metadata=metadata
            )
        },
    )
    return build_app(tree, authentication={"single_user_api_key": "secret"})
    # In this module we use a raw TestClient instead of tiled.client to omit
    # tiled.client's default headers and other configuration.


def test_gzip_supported(app):
    with TestClient(app=app) as client:
        client.headers["Authorization"] = "Apikey secret"
        client.headers["Accept-Encoding"] = "gzip"
        metadata_response = client.get("/api/v1/search")
        data_response = client.get(
            "/api/v1/array/full/compresses_well", headers={"Accept": "text/csv"}
        )
    assert metadata_response.status_code == HTTP_200_OK
    assert data_response.status_code == HTTP_200_OK
    assert "gzip" in metadata_response.headers["Content-Encoding"]
    assert "gzip" in data_response.headers["Content-Encoding"]


def test_zstd_preferred(app):
    with TestClient(app=app) as client:
        client.headers["Authorization"] = "Apikey secret"
        client.headers["Accept-Encoding"] = "zstd"
        metadata_response = client.get("/api/v1/search")
        data_response = client.get(
            "/api/v1/array/full/compresses_well", headers={"Accept": "text/csv"}
        )
    assert metadata_response.status_code == HTTP_200_OK
    assert data_response.status_code == HTTP_200_OK
    assert "zstd" in metadata_response.headers["Content-Encoding"]
    assert "zstd" in data_response.headers["Content-Encoding"]
