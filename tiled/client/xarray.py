import threading

import dask
import dask.array
import pandas
import xarray

from ..serialization.table import deserialize_arrow
from ..structures.core import Spec
from ..utils import APACHE_ARROW_FILE_MIME_TYPE
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
            self.context.http_client.get, self.item["links"]["full"]
        )
        data_vars_fetcher = _WideTableFetcher(
            self.context.http_client.get, self.item["links"]["full"]
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
                    )
                elif "xarray_data_var" in spec_names:
                    data_vars[name] = (
                        array_client.dims,
                        data_vars_fetcher.register(name, array_client, array_structure),
                    )
                else:
                    raise ValueError(
                        "Child nodes of xarray_dataset should include spec "
                        "'xarray_coord' or 'xarray_data_var'."
                    )
            else:
                if "xarray_coord" in spec_names:
                    coords[name] = (array_client.dims, array_client.read())
                elif "xarray_data_var" in spec_names:
                    data_vars[name] = (array_client.dims, array_client.read())
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


URL_CHARACTER_LIMIT = 2000  # number of characters
_EXTRA_CHARS_PER_ITEM = len("&field=")


class _WideTableFetcher:
    def __init__(self, get, link):
        self.get = get
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
                # HTTP stacks (e.g. nginx). The HTTP spec does not define a limit,
                # but a common setting is 4K or 8K (for all the headers together).
                # As another reference point, Internet Explorer imposes a
                # 2048-character limit on URLs.
                variables = []
                dataframes = []
                budget = URL_CHARACTER_LIMIT
                budget -= len(self.link)
                # Fetch the variables in batches.
                for variable in self.variables:
                    budget -= _EXTRA_CHARS_PER_ITEM + len(variable)
                    if budget < 0:
                        # Fetch a batch and then add `variable` to the next batch.
                        dataframes.append(self._fetch_variables(variables))
                        variables.clear()
                        budget = URL_CHARACTER_LIMIT - (
                            _EXTRA_CHARS_PER_ITEM + len(variable)
                        )
                    variables.append(variable)
                if variables:
                    # Fetch the final batch.
                    dataframes.append(self._fetch_variables(variables))
                self._dataframe = pandas.concat(dataframes, axis=1).reset_index()
        return self._dataframe

    def _fetch_variables(self, variables):
        content = handle_error(
            self.get(
                self.link,
                params={"format": APACHE_ARROW_FILE_MIME_TYPE, "field": variables},
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
