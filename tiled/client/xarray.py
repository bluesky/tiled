import math
import threading

import dask
import dask.array
import xarray

from ..serialization.table import deserialize_arrow
from ..structures.core import Spec
from ._protocol.dataset import wide_table_batch, xarray_variable_role
from ._protocol.table import draft_request_read_wide_table
from .container import Container
from .utils import send_request_draft


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
        # Track which variables use direct fetch (not wide-table optimization).
        # These are the ones whose dask arrays will advance the progress bar.
        # Returned as a value (not stored on self) to avoid shared-state issues
        # if two threads call read() on the same client concurrently.
        direct_fetch_names = set()
        # Optimization: download scalar/short variables batched as one
        # DataFrame, fetched on first access.
        fetchers = {
            "coord": _WideTableFetcher(self.context, self.item["links"]["full"]),
            "data_var": _WideTableFetcher(self.context, self.item["links"]["full"]),
        }
        array_clients = {}
        array_structures = {}
        for name, array_client in self.items():
            if (variables is not None) and (name not in variables):
                continue
            array_clients[name] = array_client
            array_structures[name] = array_client.structure()
        batched = wide_table_batch(
            {name: structure.shape for name, structure in array_structures.items()},
            enabled=optimize_wide_table,
        )
        for name, array_client in array_clients.items():
            role = xarray_variable_role(set(spec.name for spec in array_client.specs))
            if name in batched:
                values = fetchers[role].register(
                    name, array_client, array_structures[name]
                )
            else:
                direct_fetch_names.add(name)
                values = array_client.read()
            target = coords if role == "coord" else data_vars
            target[name] = (array_client.dims, values, array_client.metadata["attrs"])
        return data_vars, coords, direct_fetch_names

    def read(self, variables=None, *, optimize_wide_table=True):
        data_vars, coords, _ = self._build_arrays(variables, optimize_wide_table)
        return xarray.Dataset(
            data_vars=data_vars, coords=coords, attrs=self.metadata["attrs"]
        )


class DatasetClient(DaskDatasetClient):
    def read(self, variables=None, *, optimize_wide_table=True):
        data_vars, coords, direct_fetch_names = self._build_arrays(
            variables, optimize_wide_table
        )
        ds = xarray.Dataset(
            data_vars=data_vars, coords=coords, attrs=self.metadata["attrs"]
        )
        # Count fetch tasks only for direct-fetch variables (not wide-table).
        # Wide-table variables use dask.delayed and don't advance the bar.
        total = 0
        for name, var in list(ds.data_vars.items()) + list(ds.coords.items()):
            if name in direct_fetch_names and dask.is_dask_collection(var.data):
                total += math.prod(len(c) for c in var.data.chunks)
        with self.context.tracking_progress(total):
            return ds.load()


class _WideTableFetcher:
    def __init__(self, context, link):
        self.context = context
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
                draft = draft_request_read_wide_table(self.link, self.variables)
                content = send_request_draft(self.context, draft).read()
                self._dataframe = deserialize_arrow(content).reset_index()
        return self._dataframe


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
