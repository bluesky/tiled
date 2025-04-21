import time
from urllib.parse import parse_qs, urlparse

from ..structures.core import StructureFamily
from .container import LENGTH_CACHE_TTL, Container
from .utils import MSGPACK_MIME_TYPE, client_for_item, handle_error, retry_context


class Composite(Container):
    def get_contents(self, maxlen=None, include_metadata=False):
        result = {}
        next_page_url = f"{self.item['links']['search']}"
        while (next_page_url is not None) or (
            maxlen is not None and len(result) < maxlen
        ):
            for attempt in retry_context():
                with attempt:
                    content = handle_error(
                        self.context.http_client.get(
                            next_page_url,
                            headers={"Accept": MSGPACK_MIME_TYPE},
                            params={
                                **parse_qs(urlparse(next_page_url).query),
                                **self._queries_as_params,
                            }
                            | ({} if include_metadata else {"select_metadata": False})
                            | (
                                {}
                                if not self._include_data_sources
                                else {"include_data_sources": True}
                            ),
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

    def __iter__(self):
        yield from self._keys_slice(0, None, 1)

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

    def __contains__(self, key):
        return key in self._flat_keys_mapping.keys()

    def create_container(self, key=None, *, metadata=None, specs=None):
        """Composite nodes can not include nested containers by design."""
        raise NotImplementedError("Cannot create a container within a composite node.")

    def create_composite(self, key=None, *, metadata=None, specs=None):
        """Composite nodes can not include nested composites by design."""
        raise NotImplementedError("Cannot create a composite within a composite node.")

    def read(self, variables=None, dim0=None):
        """Download the contents of a composite node as an xarray.Dataset.

        Parameters
        ----------
        variables (list, optional) : List of variable names to read. If None, all
            variables are read. Defaults to None.
        dim0 (str, optional) : Name of the dimension to use for the first dimension;
            if None (default), each array will have its own dimension name. The dims tuple,
            if specified in the structure, takes precedence over this.

        Returns
        -------
        xarray.Dataset: The dataset containing the requested variables.
        """
        import pandas
        import xarray

        array_dims = {}
        data_vars = {}
        for part, item in self.get_contents().items():
            # Read all or selective arrays/columns.
            if item["attributes"]["structure_family"] in {
                StructureFamily.array,
                StructureFamily.sparse,
            }:
                if (variables is None) or (part in variables):
                    array_client = self.parts[part]
                    data_vars[part] = array_client.read()  # [Dask]ArrayClient
                    array_dims[part] = array_client.dims
            elif item["attributes"]["structure_family"] == StructureFamily.awkward:
                if (variables is None) or (part in variables):
                    try:
                        data_vars[part] = self.parts[part].read().to_numpy()
                    except ValueError as e:
                        raise ValueError(
                            f"Failed to convert awkward array to numpy: {e}"
                        ) from e
            elif item["attributes"]["structure_family"] == StructureFamily.table:
                # For now, greedily load tabular data. We cannot know the shape
                # of the columns without reading them. Future work may enable
                # this to be lazy.
                table_client = self.parts[part]
                columns = set(variables or table_client.columns).intersection(
                    table_client.columns
                )
                df = table_client.read(list(columns))
                for column in columns:
                    data_vars[column] = df[column].values
                    # Convert (experimental) pandas.StringDtype to numpy's unicode string dtype
                    if isinstance(data_vars[column].dtype, pandas.StringDtype):
                        data_vars[column] = data_vars[column].astype("U")
            else:
                raise ValueError(
                    f"Unsupported structure family: {item['attributes']['structure_family']}"
                )

        # Create xarray.Dataset from the data_vars dictionary
        is_dim0_consistent = (
            len(
                set(
                    [
                        arr.shape[0]
                        for var_name, arr in data_vars.items()
                        if arr.ndim > 0 and not array_dims.get(var_name)
                    ]
                )
            )
            <= 1
        )
        if dim0 is not None and not is_dim0_consistent:
            raise ValueError(
                "Cannot specify dim0 when the arrays have different left-most dimensions."
            )

        for var_name, arr in data_vars.items():
            if is_dim0_consistent:
                dims = (dim0 or "dim0",) + tuple(
                    f"{var_name}_dim{i+1}" for i in range(len(arr.shape) - 1)
                )
            else:
                dims = tuple(f"{var_name}_dim{i}" for i in range(len(arr.shape)))
            data_vars[var_name] = array_dims.get(var_name) or dims, arr

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
