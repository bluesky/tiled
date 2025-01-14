import numpy
from pydantic import Secret

from tiled.adapters.array import ArrayAdapter
from tiled.structures.core import StructureFamily


class MockClient:
    
    def __init__(self, base_url: str, example_token: str = "secret"):
        self._base_url = base_url
        # This stands in for a secret token issued by the external service.
        self._example_token = Secret(example_token)

    # This API (get_contents, get_metadata, get_data) is just made up and not important.
    # Could be anything.

    async def get_metadata(self, url, token):
        # This assert stands in for the mocked service
        # authenticating a request.
        assert token == self._example_token.get_secret_value()
        return {"metadata": str(url)}

    async def get_contents(self, url, token):
        # This assert stands in for the mocked service
        # authenticating a request.
        assert token == self._example_token.get_secret_value()
        return ["a", "b", "c"]

    async def get_data(self, url, token):
        # This assert stands in for the mocked service
        # authenticating a request.
        assert token == self._example_token.get_secret_value()
        return numpy.ones((3, 3))


class Adapter:
    """
    This is the Adapter that should be given to the Tiled service in config.

    The only API it should implement is `with_session_state`, which returns
    the AuthenticatedAdapter that actually accesses the data.
    """

    def __init__(self, base_url, metadata=None):
        self.client = MockClient(base_url)
        self.metadata = metadata

    def with_session_state(self, state):
        return AuthenticatedAdapter(self.client, state["token"], metadata=self.metadata)


class AuthenticatedAdapter:
    structure_family = StructureFamily.container

    def __init__(self, client, token, segments=None, metadata=None):
        self._client = client
        self._token = token
        self._segments = segments or []
        self.metadata = metadata or {}

    async def lookup_adapter(self, segments):
        # The service URL is probably something based on segments, but not
        # literally this....
        metadata_url = "/".join(self._segments + segments)
        data_url = "/".join(self._segments + segments)
        metadata = await self._client.get_metadata(metadata_url, token=self._token)
        data = await self._client.get_data(data_url, token=self._token)
        # This could alternatively be some file-based adapter with HDF5Adapter
        # or something custom, or another AuthenticatedAdapter...
        return ArrayAdapter.from_array(data, metadata=metadata)

    async def keys_range(self, offset, limit):
        url = ...  # based on self._segments
        return await self._client.get_contents(url, token=self._token)
        return ["a", "b", "c"]

    async def items_range(self, offset, limit):
        # Ideally this would be a batched request to the external service.
        return [
            (key, self.lookup_adapter([key])) for key in self._keys_range(offset, limit)
        ]
