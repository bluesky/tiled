from catalog_server.client.catalog import ClientCatalog


class BlueskyRun(ClientCatalog):
    "A ClientCatalog with a custom repr and (eventually) helper methods"

    def __repr__(self):
        return f"<{type(self).__name__}(uid={self.metadata['start']['uid']})>"
