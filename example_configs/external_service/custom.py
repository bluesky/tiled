import numpy

from tiled.adapters.array import ArrayAdapter
from tiled.authenticators import UserSessionState
from tiled.server.protocols import InternalAuthenticator
from tiled.structures.core import StructureFamily


class Authenticator(InternalAuthenticator):
    "This accepts any password and stashes it in session state as 'token'."

    async def authenticate(self, username: str, password: str) -> UserSessionState:
        return UserSessionState(username, {"token": password})


# This stands in for a secret token issued by the external service.
SERVICE_ISSUED_TOKEN = "secret"


class MockClient:
    def __init__(self, base_url):
        self.base_url = base_url

    # This API (get_contents, get_metadata, get_data) is just made up and not important.
    # Could be anything.

    async def get_metadata(self, url, token):
        # This assert stands in for the mocked service
        # authenticating a request.
        assert token == SERVICE_ISSUED_TOKEN
        return {"metadata": str(url)}

    async def get_contents(self, url, token):
        # This assert stands in for the mocked service
        # authenticating a request.
        assert token == SERVICE_ISSUED_TOKEN
        return ["a", "b", "c"]

    async def get_data(self, url, token):
        # This assert stands in for the mocked service
        # authenticating a request.
        assert token == SERVICE_ISSUED_TOKEN
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

    async def items_range(self, offset, limit):
        # Ideally this would be a batched request to the external service.
        result = []
        for key in self._keys_range(offset, limit):
            try:
                result.append((key, self.lookup_adapter([key])))
            except KeyError:
                result.append((key, None))  # for backcompatibility
        return result
