import json

from catalog_server.client.catalog import ClientCatalog


class BlueskyRun(ClientCatalog):
    "A ClientCatalog with a custom repr and (eventually) helper methods"

    def __repr__(self):
        return (
            f"<{type(self).__name__}("
            f"uid={self.metadata['start']['uid']!r}, "
            f"streams={set(self)!r}"
            ")>"
        )

    def documents(self):
        # (name, doc) pairs are streamed as newline-delimited JSON
        with self._client.stream(
            "GET", f"/documents/{'/'.join(self._path)}"
        ) as response:
            for line in response.iter_lines():
                yield tuple(json.loads(line))


class BlueskyEventStream(ClientCatalog):
    def __repr__(self):
        return f"<{type(self).__name__}>"

    @property
    def descriptors(self):
        return self.metadata["descriptors"]
