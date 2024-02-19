import io

import h5py
import pytest

from ..catalog import in_memory
from ..catalog.register import register
from ..client import Context, from_context, record_history
from ..examples.generate_files import generate_files
from ..server.app import build_app


@pytest.fixture
def example_data_dir(tmpdir_factory):
    """
    Generate a temporary directory with example files.

    The tmpdir_factory fixture ensures that this directory is cleaned up at test exit.
    """
    tmpdir = tmpdir_factory.mktemp("example_files")
    generate_files(tmpdir)
    return tmpdir


@pytest.fixture
async def awaitable_client_generator(example_data_dir):
    catalog = in_memory(readable_storage=[example_data_dir])
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(catalog, example_data_dir)
        yield client


@pytest.fixture
async def awaitable_client(awaitable_client_generator):
    return await awaitable_client_generator.__anext__()


@pytest.mark.asyncio
@pytest.mark.parametrize("fields", (None, (), ("a", "b")))
async def test_directory_fields(awaitable_client, fields):
    client = await awaitable_client
    url_path = client.item["links"]["full"]
    buffer = io.BytesIO()
    with record_history() as history:
        client.export(buffer, fields=fields, format="application/x-hdf5")

    # Directory contents were fetched as a single request using "full" route
    (request,) = history.requests
    request_url_path = request.url.copy_with(query=None)
    assert request_url_path == url_path

    # Only the requested fields were fetched
    file = h5py.File(buffer, "r")
    actual_fields = set(file.keys())
    expected = set(fields or client.keys())  # By default all fields were fetched
    assert actual_fields == expected
