from urllib.parse import parse_qs, urlparse

from ..structures.core import StructureFamily
from ..utils import node_repr
from .container import Container
from .utils import MSGPACK_MIME_TYPE, ClientError, client_for_item, handle_error

LENGTH_CACHE_TTL = 1  # second


class CompositeClient(Container):
    def __repr__(self):
        # Display up to the first N flat_keys from the inlined structure.
        N = 10
        return node_repr(self, self._keys_slice(0, N, direction=1))

    @property
    def parts(self):
        self.refresh()
        structure = self.structure()
        if structure and structure.contents:
            return CompositeContents(self)

    def __len__(self) -> int:
        # If the contents of this node was provided in-line, there is an
        # implication that the contents are not expected to be dynamic. Used the
        # count provided in the structure.
        if self.structure() and (self.structure().count is not None):
            return self.structure().count
        return 0

    def __iter__(self):
        # If the contents of this node was provided in-line, and we don't need
        # to apply any filtering or sorting, we can slice the in-lined data
        # without fetching anything from the server.

        # structure = self.structure()
        # if structure and structure.contents:
        #     return (yield from structure.contents)
        return (yield from self._items_slice(start=0, stop=None, direction=1))

    def __getitem__(self, key):
        if key not in self.structure().flat_keys:
            # Only allow getting from flat_keys, not parts
            raise KeyError(key)
        try:
            self_link = self.item["links"]["self"].rstrip("/")
            url_path = f"{self_link}/{key}"
            params = parse_qs(urlparse(url_path).query)
            if self._include_data_sources:
                params["include_data_sources"] = True
            content = handle_error(
                self.context.http_client.get(
                    url_path,
                    headers={"Accept": MSGPACK_MIME_TYPE},
                    params=params,
                )
            ).json()
        except ClientError as err:
            if err.response.status_code == 404:
                raise KeyError(key)
            raise
        item = content["data"]
        return client_for_item(
            self.context,
            self.structure_clients,
            item,
            include_data_sources=self._include_data_sources,
        )

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(self, start, stop, direction):
        # If the contents of this node was provided in-line (default),
        # we can slice the in-lined data without fetching anything from the server.
        self.refresh()
        contents = self.item["attributes"]["structure"]["contents"]
        if contents is not None:
            keys = []
            for key, item in contents.items():
                if item["attributes"]["structure_family"] == StructureFamily.table:
                    keys.extend(item["attributes"]["structure"]["columns"])
                else:
                    keys.append(key)
            if direction < 0:
                keys = list(reversed(keys))
            return (yield from keys[start:stop])

    def _items_slice(self, start, stop, direction):
        # If the contents of this node was provided in-line (default),
        # we can slice the in-lined data without fetching anything from the server.
        self.refresh()
        contents = self.item["attributes"]["structure"]["contents"]
        if contents is not None:
            lazy_items = []
            for key, item in contents.items():
                if item["attributes"]["structure_family"] == StructureFamily.table:
                    for col in item["attributes"]["structure"]["columns"]:
                        lazy_items.append((col, lambda c: self[c]))
                else:
                    lazy_items.append(
                        (
                            key,
                            lambda c: client_for_item(
                                self.context,
                                self.structure_clients,
                                contents[c],
                                include_data_sources=self._include_data_sources,
                            ),
                        )
                    )

            if direction < 0:
                lazy_items = list(reversed(lazy_items))
            # breakpoint()
            for key, lazy_item in lazy_items[start:stop]:
                yield key, lazy_item(key)
            return


class CompositeContents:
    def __init__(self, node):
        self.contents = node.structure().contents
        self.links = node.item["links"]
        self.context = node.context
        self.structure_clients = node.structure_clients
        self._include_data_sources = node._include_data_sources

    def __repr__(self):
        return (
            f"<{type(self).__name__} {{"
            + ", ".join(f"'{item}'" for item in self.contents)
            + "}>"
        )

    def __getitem__(self, key):
        if key not in self.contents:
            raise KeyError(key)

        return client_for_item(
            self.context,
            self.structure_clients,
            self.contents[key],
            include_data_sources=self._include_data_sources,
        )

    def __iter__(self):
        for key in self.contents:
            yield key

    def __len__(self) -> int:
        return len(self.contents)
