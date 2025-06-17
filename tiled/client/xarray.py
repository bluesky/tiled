import threading
from urllib.parse import parse_qs, urlparse

import dask
import dask.array
import xarray

from ..serialization.table import deserialize_arrow
from ..structures.core import Spec
from ..utils import APACHE_ARROW_FILE_MIME_TYPE
from .base import BaseClient
from .container import Container
from .utils import handle_error

LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION = 1_000_000


class DaskDatasetClient(Container):
    def _repr_pretty_(self, p, cycle):
        """
        Provide "pretty" display in IPython/Jupyter.

        See https://ipython.readthedocs.io/en/stable/config/integrating.html#rich-display
        """
        p.text(f"<{type(self).__name__} {list(self)}>")

    def _ipython_key_completions_(self):
        """
        Provide method for the key-autocompletions in IPython.

        See http://ipython.readthedocs.io/en/stable/config/integrating.html#tab-completion
        """
        return list(self)

    def _build_arrays(self, variables, optimize_wide_table):
        data_vars = {}
        coords = {}
        # Optimization: Download scalar columns in batch as DataFrame.
        # on first access.
        coords_fetcher = _WideTableFetcher(
            self.context.http_client,
            self.item["links"]["full"],
        )
        data_vars_fetcher = _WideTableFetcher(
            self.context.http_client,
            self.item["links"]["full"],
        )
        array_clients = {}
        array_structures = {}
        first_dims = []
        for name, array_client in self.items():
            if (variables is not None) and (name not in variables):
                continue
            array_clients[name] = array_client
            array_structure = array_client.structure()
            array_structures[name] = array_structure
            if array_structure.shape:
                first_dims.append(array_structure.shape[0])
            else:
                first_dims.append(None)
        if len(set(first_dims)) > 1:
            # ragged, not tabular
            optimize_wide_table = False
        for name, array_client in array_clients.items():
            array_structure = array_structures[name]
            shape = array_structure.shape
            spec_names = set(spec.name for spec in array_client.specs)

            if optimize_wide_table and (
                (not shape)  # empty
                or (
                    (shape[0] < LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION)
                    and (len(shape) < 2)
                )
            ):
                if "xarray_coord" in spec_names:
                    coords[name] = (
                        array_client.dims,
                        coords_fetcher.register(name, array_client, array_structure),
                        array_client.metadata["attrs"],
                    )
                elif "xarray_data_var" in spec_names:
                    data_vars[name] = (
                        array_client.dims,
                        data_vars_fetcher.register(name, array_client, array_structure),
                        array_client.metadata["attrs"],
                    )
                else:
                    raise ValueError(
                        "Child nodes of xarray_dataset should include spec "
                        "'xarray_coord' or 'xarray_data_var'."
                    )
            else:
                if "xarray_coord" in spec_names:
                    coords[name] = (
                        array_client.dims,
                        array_client.read(),
                        array_client.metadata["attrs"],
                    )
                elif "xarray_data_var" in spec_names:
                    data_vars[name] = (
                        array_client.dims,
                        array_client.read(),
                        array_client.metadata["attrs"],
                    )
                else:
                    raise ValueError(
                        "Child nodes of xarray_dataset should include spec "
                        "'xarray_coord' or 'xarray_data_var'."
                    )
        return data_vars, coords

    def read(self, variables=None, *, optimize_wide_table=True):
        data_vars, coords = self._build_arrays(variables, optimize_wide_table)
        return xarray.Dataset(
            data_vars=data_vars, coords=coords, attrs=self.metadata["attrs"]
        )


class DatasetClient(DaskDatasetClient):
    def read(self, variables=None, *, optimize_wide_table=True):
        return (
            super()
            .read(variables=variables, optimize_wide_table=optimize_wide_table)
            .load()
        )


_EXTRA_CHARS_PER_ITEM = len("&field=")


class _WideTableFetcher:
    def __init__(self, http_client, link):
        self.http_client = http_client
        self.link = link
        self.variables = []
        self._dataframe = None
        # This lock ensures that multiple threads (e.g. dask worker threads)
        # do not prompts us to re-request the same data. Only the first worker
        # to ask for the data should trigger a request.
        self._lock = threading.Lock()

    def register(self, name, array_client, array_structure):
        if self._dataframe is not None:
            raise RuntimeError("Cannot add variables; already fetched.")
        self.variables.append(name)
        # TODO Can we avoid .values here?
        return dask.array.from_delayed(
            dask.delayed(self.dataframe)()[name].values,
            shape=array_structure.shape,
            dtype=array_structure.data_type.to_numpy_dtype(),
        )

    def dataframe(self):
        with self._lock:
            if self._dataframe is None:
                # If self.variables contains many and/or lengthy names,
                # we can bump into the URI size limit commonly imposed by
                # HTTP stacks (e.g. nginx).
                url_length_for_get_request = len(self.link) + sum(
                    _EXTRA_CHARS_PER_ITEM + len(variable) for variable in self.variables
                )
                if url_length_for_get_request > BaseClient.URL_CHARACTER_LIMIT:
                    dataframe = self._fetch_variables(self.variables, "POST")
                else:
                    dataframe = self._fetch_variables(self.variables, "GET")
                self._dataframe = dataframe.reset_index()
        return self._dataframe

    def _fetch_variables(self, variables, method="GET"):
        if method == "GET":
            return self._fetch_variables__get(variables)
        if method == "POST":
            return self._fetch_variables__post(variables)
        raise NotImplementedError(f"Method {method} is not supported")

    def _fetch_variables__get(self, variables):
        content = handle_error(
            self.http_client.get(
                self.link,
                params={
                    **parse_qs(urlparse(self.link).query),
                    "format": APACHE_ARROW_FILE_MIME_TYPE,
                    "field": variables,
                },
            )
        ).read()
        return deserialize_arrow(content)

    def _fetch_variables__post(self, variables):
        content = handle_error(
            self.http_client.post(
                self.link,
                json=variables,
                params={
                    **parse_qs(urlparse(self.link).query),
                    "format": APACHE_ARROW_FILE_MIME_TYPE,
                },
            )
        ).read()
        return deserialize_arrow(content)


def write_xarray_dataset(client_node, dataset, key=None):
    dataset_client = client_node.create_container(
        key=key, specs=[Spec("xarray_dataset")], metadata={"attrs": dataset.attrs}
    )
    for name in dataset.data_vars:
        data_array = dataset[name]
        dataset_client.write_array(
            data_array.data,
            key=name,
            metadata={"attrs": data_array.attrs},
            dims=data_array.dims,
            specs=[Spec("xarray_data_var")],
        )
    for name in dataset.coords:
        data_array = dataset[name]
        dataset_client.write_array(
            data_array.data,
            key=name,
            metadata={"attrs": data_array.attrs},
            dims=data_array.dims,
            specs=[Spec("xarray_coord")],
        )
    return dataset_client
