import msgpack

from ..utils import DictView, ListView
from .authentication import reauthenticate_client
from .utils import (
    handle_error,
    NEEDS_INITIALIZATION,
    NotAvailableOffline,
    UNSET,
)
from ..catalogs.utils import UNCHANGED


class BaseClient:
    def __init__(
        self,
        client,
        *,
        item,
        username,
        token_cache,
        cache,
        offline,
        path,
        metadata,
        params,
    ):
        self._client = client
        self._token_cache = token_cache
        self._username = username
        self._offline = offline
        if isinstance(path, str):
            raise ValueError("path is expected to be a list of segments")
        # Stash *immutable* copies just to be safe.
        self._path = tuple(path or [])
        if item is NEEDS_INITIALIZATION:
            self._item = None
            self._metadata = {}
        else:
            self._item = item
            self._metadata = metadata
        self._cached_len = None  # a cache just for __len__
        self._params = params or {}
        self._cache = cache
        super().__init__()

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @property
    def item(self):
        "JSON payload describing this item. Mostly for internal use."
        return self._item

    @property
    def metadata(self):
        "Metadata about this data source."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    @property
    def path(self):
        "Sequence of entry names from the root Catalog to this entry"
        return ListView(self._path)

    @property
    def uri(self):
        "Direct link to this entry"
        return self.item["links"]["self"]

    @property
    def username(self):
        return self._username

    def new_variation(
        self,
        offline=UNCHANGED,
        cache=UNCHANGED,
        metadata=UNCHANGED,
        path=UNCHANGED,
        params=UNCHANGED,
        **kwargs,
    ):
        """
        This is intended primarily for internal use and use by subclasses.
        """
        if offline is UNCHANGED:
            offline = self._offline
        if cache is UNCHANGED:
            cache = self._cache
        if metadata is UNCHANGED:
            metadata = self._metadata
        if path is UNCHANGED:
            path = self._path
        if params is UNCHANGED:
            params = self._params
        return type(self)(
            client=self._client,
            item=self._item,
            username=self._username,
            offline=offline,
            cache=cache,
            metadata=metadata,
            path=path,
            params=params,
            token_cache=self._token_cache,
            **kwargs,
        )

    def _get_content_with_cache(self, path, accept=None, timeout=UNSET, **kwargs):
        request = self._client.build_request("GET", path, **kwargs)
        if accept:
            request.headers["Accept"] = accept
        url = request.url.raw  # URL as tuple
        if self._offline:
            # We must rely on the cache alone.
            reservation = self._cache.get_reservation(url)
            if reservation is None:
                raise NotAvailableOffline(url)
            content = reservation.load_content()
            if content is None:
                # TODO Do we ever get here?
                raise NotAvailableOffline(url)
            return content
        if self._cache is None:
            # No cache, so we can use the client straightforwardly.
            response = self._send(request, timeout=timeout)
            handle_error(response)
            return response.content
        # If we get this far, we have an online client and a cache.
        reservation = self._cache.get_reservation(url)
        try:
            if reservation is not None:
                request.headers["If-None-Match"] = reservation.etag
            response = self._send(request, timeout=timeout)
            handle_error(response)
            if response.status_code == 304:  # HTTP 304 Not Modified
                # Read from the cache
                content = reservation.load_content()
            elif response.status_code == 200:
                etag = response.headers.get("ETag")
                content = response.content
                # TODO Respect Cache-control headers (e.g. "no-store")
                if etag is not None:
                    # Write to cache.
                    self._cache.put_etag_for_url(url, etag)
                    self._cache.put_content(etag, content)
            else:
                raise NotImplementedError(
                    f"Unexpected status_code {response.status_code}"
                )
        finally:
            if reservation is not None:
                reservation.ensure_released()
        return content

    def _get_json_with_cache(self, path, **kwargs):
        return msgpack.unpackb(
            self._get_content_with_cache(path, accept="application/x-msgpack", **kwargs)
        )

    def _send(self, request, timeout, attempts=0):
        """
        Handle httpx's timeout API, which uses a special internal sentinel to mean
        "no timeout" and therefore must not be passed any value (including None)
        if we want no timeout.
        """
        if timeout is UNSET:
            response = self._client.send(request)
        else:
            response = self._client.send(request, timeout=timeout)
        if (response.status_code == 401) and (attempts == 0):
            # Try refreshing the token.
            # TODO Use a more targeted signal to know that refreshing the token will help.
            # Parse the error message? Add a special header from the server?
            if self._username is not None:
                reauthenticate_client(
                    self._client,
                    self._username,
                    token_cache=self._token_cache,
                )
                request.headers["authorization"] = self._client.headers["authorization"]
                return self._send(request, timeout, attempts=1)
        return response


class BaseStructureClient(BaseClient):
    def __init__(
        self,
        client,
        *,
        structure=None,
        **kwargs,
    ):
        super().__init__(client, **kwargs)
        self._structure = structure

    def new_variation(self, structure=UNCHANGED, **kwargs):
        if structure is UNCHANGED:
            structure = self._structure
        return super().new_variation(structure=structure, **kwargs)

    def touch(self):
        """
        Access all the data.

        This causes it to be cached if the client is configured with a cache.
        """
        repr(self)
        self.read()

    def structure(self):
        """
        Return a dataclass describing the structure of the data.
        """
        # This is implemented by subclasses.
        pass


class BaseArrayClient(BaseStructureClient):
    """
    Shared by Array, DataArray, Dataset

    Subclass must define:

    * STRUCTURE_TYPE : type
    """

    def structure(self):
        # Notice that we are NOT *caching* in self._structure here. We are
        # allowing that the creator of this instance might have already known
        # our structure (as part of the some larger structure) and passed it
        # in.
        if self._structure is None:
            content = self._get_json_with_cache(
                f"/metadata/{'/'.join(self._path)}",
                params={
                    "fields": ["structure.micro", "structure.macro"],
                    **self._params,
                },
            )
            result = content["data"]["attributes"]["structure"]
            structure = self.STRUCTURE_TYPE.from_json(result)
        else:
            structure = self._structure
        return structure
