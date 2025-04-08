import time
from urllib.parse import parse_qs, urlparse

from ..structures.core import StructureFamily
from .container import LENGTH_CACHE_TTL, Container
from .utils import MSGPACK_MIME_TYPE, client_for_item, handle_error


class Composite(Container):
    def get_contents(self, maxlen=None, include_metadata=False):
        result = {}
        next_page_url = f"{self.item['links']['search']}"
        while (next_page_url is not None) or (
            maxlen is not None and len(result) < maxlen
        ):
            content = handle_error(
                self.context.http_client.get(
                    next_page_url,
                    headers={"Accept": MSGPACK_MIME_TYPE},
                    params={
                        **parse_qs(urlparse(next_page_url).query),
                        **self._queries_as_params,
                    }
                    | ({} if include_metadata else {"select_metadata": False}),
                )
            ).json()
            result.update({item["id"]: item for item in content["data"]})

            next_page_url = content["links"]["next"]

        return result

    @property
    def _flat_keys_mapping(self):
        result = {}
        for key, item in self.get_contents().items():
            if item["attributes"]["structure_family"] == StructureFamily.table:
                for col in item["attributes"]["structure"]["columns"]:
                    result[col] = item["id"] + "/" + col
            else:
                result[item["id"]] = item["id"]

        self._cached_len = (len(result), time.monotonic() + LENGTH_CACHE_TTL)

        return result

    @property
    def parts(self):
        return CompositeParts(self)

    def _keys_slice(self, start, stop, direction, _ignore_inlined_contents=False):
        yield from self._flat_keys_mapping.keys()

    def _items_slice(self, start, stop, direction, _ignore_inlined_contents=False):
        for key in self._flat_keys_mapping.keys():
            yield key, self[key]

    def __len__(self):
        if self._cached_len is not None:
            length, deadline = self._cached_len
            if time.monotonic() < deadline:
                # Used the cached value and do not make any request.
                return length

        return len(self._flat_keys_mapping)

    def __getitem__(self, key: str, _ignore_inlined_contents=False):
        if isinstance(key, tuple):
            key = "/".join(key)
        if key in self._flat_keys_mapping:
            key = self._flat_keys_mapping[key]
        else:
            raise KeyError(
                f"Key '{key}' not found. If it refers to a table, use .parts['{key}'] instead."
            )

        return super().__getitem__(key, _ignore_inlined_contents)

    def create_container(self, key=None, *, metadata=None, specs=None):
        """Composite nodes can not include nested containers by design."""
        raise NotImplementedError("Cannot create a container within a composite node.")

    def create_composite(self, key=None, *, metadata=None, specs=None):
        """Composite nodes can not include nested composites by design."""
        raise NotImplementedError("Cannot create a composite within a composite node.")

    def read(self, variables=None):
        import xarray

        if variables is None:
            # Read all.
            data_vars = {}
            for part, item in self.get_contents().items():
                if item["attributes"]["structure_family"] != StructureFamily.table:
                    data_vars[part] = self.parts[part].read()  # [Dask]ArrayClient
                else:
                    # For now, greedily load tabular data. We cannot know the shape
                    # of the columns without reading them. Future work may enable
                    # this to be lazy.
                    table_client = self.parts[part]
                    df = table_client.read()
                    for column in table_client.columns:
                        data_vars[column] = df[column].values
        else:
            # Read selective arrays/columns.
            data_vars = {}
            for part, item in self.get_contents().items():
                if item["attributes"]["structure_family"] != StructureFamily.table:
                    if part in variables:
                        data_vars[part] = self.parts[part].read()  # [Dask]ArrayClient
                else:
                    # For now, greedily load tabular data. We cannot know the shape
                    # of the columns without reading them. Future work may enable
                    # this to be lazy.
                    table_client = self.parts[part]
                    columns = set(variables).intersection(table_client.columns)
                    df = table_client.read(list(columns))
                    for column in columns:
                        data_vars[column] = df[column].values

        return xarray.Dataset(data_vars=data_vars)


class CompositeParts:
    def __init__(self, node):
        self.contents = node.get_contents(include_metadata=True)
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
        key, *tail = key.split("/")

        if key not in self.contents:
            raise KeyError(key)

        client = client_for_item(
            self.context,
            self.structure_clients,
            self.contents[key],
            include_data_sources=self._include_data_sources,
        )

        if tail:
            return client["/".join(tail)]
        else:
            return client

    def __iter__(self):
        for key in self.contents:
            yield key

    def __len__(self) -> int:
        return len(self.contents)
