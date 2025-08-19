import time
from typing import Iterable, Optional, Union
from urllib.parse import parse_qs, urlparse

from ..structures.core import StructureFamily
from .container import LENGTH_CACHE_TTL, Container
from .utils import MSGPACK_MIME_TYPE, handle_error, retry_context


class CompositeClient(Container):
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
    def base(self):
        "Return the base Container client instead of a CompositeClient"
        return Container(
            self.context, item=self.item, structure_clients=self.structure_clients
        )

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
                f"Key '{key}' not found. If it refers to a table, access it via "
                f"the base Container client using `.base['{key}']` instead."
            )

        return super().__getitem__(key, _ignore_inlined_contents)

    def __contains__(self, key):
        return key in self._flat_keys_mapping.keys()

    def create_container(self, key=None, *, metadata=None, specs=None):
        """Composite nodes can not include nested containers by design."""
        raise NotImplementedError("Cannot create a container within a composite node.")

    def delete_contents(
        self,
        keys: Optional[Union[str, Iterable[str]]] = None,
        external_only: bool = True,
    ) -> "CompositeClient":
        """Delete the contents of this Composite node.

        Only arrays or entire tables, not individual columns, can be deleted.

        Parameters
        ----------
        keys : str or list of str
            The key(s) to delete. If a list, all keys in the list will be deleted.
            If None (default), delete all contents.
        external_only : bool, optional
            If True, only delete externally-managed data. Defaults to True.
        """

        parts = set(self.base.keys())
        if keys is None:
            keys = parts
        keys = [keys] if isinstance(keys, str) else keys
        extra_keys = set(keys).difference(parts)
        if extra_keys:
            raise KeyError(
                f"Keys {extra_keys} not found in composite node parts. "
                "If the keys reference column names of a constituent "
                "table, deleting them is not supported. Use the `.base` "
                "accessor to the Container client to delete the entire table."
            )

        return super().delete_contents(keys, external_only=external_only)

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
                    array_client = self.base[part]
                    data_vars[part] = array_client.read()  # [Dask]ArrayClient
                    array_dims[part] = array_client.dims
            elif item["attributes"]["structure_family"] == StructureFamily.awkward:
                if (variables is None) or (part in variables):
                    try:
                        data_vars[part] = self.base[part].read().to_numpy()
                    except ValueError as e:
                        raise ValueError(
                            f"Failed to convert awkward array to numpy: {e}"
                        ) from e
            elif item["attributes"]["structure_family"] == StructureFamily.table:
                # For now, greedily load tabular data. We cannot know the shape
                # of the columns without reading them. Future work may enable
                # this to be lazy.
                table_client = self.base[part]
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

    def new(
        self,
        structure_family,
        data_sources,
        *,
        key=None,
        metadata=None,
        specs=None,
        access_tags=None,
    ):
        if key in self.keys():
            raise ValueError(f"Key '{key}' already exists in the composite node.")

        return super().new(
            structure_family,
            data_sources,
            key=key,
            metadata=metadata,
            specs=specs,
            access_tags=access_tags,
        )

    def write_dataframe(
        self, dataframe, *, key=None, metadata=None, specs=None, access_tags=None
    ):
        if set(self.keys()).intersection(dataframe.columns):
            raise ValueError(
                "DataFrame columns must not overlap with existing keys in the composite node."
            )
        return super().write_dataframe(
            dataframe, key=key, metadata=metadata, specs=specs, access_tags=access_tags
        )
