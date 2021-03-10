import json

from catalog_server.client.catalog import ClientCatalog
from .common import BlueskyEventStreamMixin, BlueskyRunMixin


class BlueskyRun(BlueskyRunMixin, ClientCatalog):
    """
    This encapsulates the data and metadata for one Bluesky 'run'.
    """

    def documents(self):
        # (name, doc) pairs are streamed as newline-delimited JSON
        with self._client.stream(
            "GET", f"/documents/{'/'.join(self._path)}"
        ) as response:
            for line in response.iter_lines():
                yield tuple(json.loads(line))


class BlueskyEventStream(BlueskyEventStreamMixin, ClientCatalog):
    pass
