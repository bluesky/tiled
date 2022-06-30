import dask
import dask.array
import pandas
import xarray

from ..client.base import BaseStructureClient
from ..serialization.dataframe import deserialize_arrow
from ..utils import APACHE_ARROW_FILE_MIME_TYPE
from .node import Node

LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION = 1_000_000


class DaskDatasetClient(Node):
    separate_coords_and_data_vars_download = True

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

    def download(self):
        super().download()
        list(self)
        self.read().compute()

    def _build_arrays(self, variables, optimize_wide_table):
        data_vars = {}
        coords = {}
        # Optimization: Download scalar columns in batch as DataFrame.
        # on first access.
        coords_fetcher = _WideTableFetcher(
            self.context.get_content, self.item["links"]["full"]
        )
        if self.separate_coords_and_data_vars_download:
            data_vars_fetcher = _WideTableFetcher(
                self.context.get_content, self.item["links"]["full"]
            )
        else:
            data_vars_fetcher = coords_fetcher
        for name, array_client in self.items():
            if (variables is not None) and (name not in variables):
                continue
            array_structure = array_client.structure()
            shape = array_structure.macro.shape
            if optimize_wide_table and (
                (not shape)  # empty
                or (
                    (shape[0] < LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION)
                    and (len(shape) < 2)
                )
            ):
                if "xarray_coord" in array_client.specs:
                    coords[name] = (
                        array_client.dims,
                        coords_fetcher.register(name, array_client, array_structure),
                    )
                elif "xarray_data_var" in array_client.specs:
                    data_vars[name] = (
                        array_client.dims,
                        data_vars_fetcher.register(name, array_client, array_structure),
                    )
                else:
                    assert False, "Expected a spec"
            else:
                if "xarray_coord" in array_client.specs:
                    coords[name] = (array_client.dims, array_client.read())
                elif "xarray_data_var" in array_client.specs:
                    data_vars[name] = (array_client.dims, array_client.read())
                else:
                    assert False, "Expected a spec"
        return data_vars, coords

    def read(self, variables=None, *, optimize_wide_table=True):
        data_vars, coords = self._build_arrays(variables, optimize_wide_table)
        return xarray.Dataset(data_vars=data_vars, coords=coords, attrs=self.metadata)


class DatasetClient(DaskDatasetClient):
    separate_coords_and_data_vars_download = False

    def download(self):
        # Do not run super().download() because DaskDatasetClient calls compute()
        # which does not apply here.
        BaseStructureClient.download(self)
        self._ipython_key_completions_()
        self.read()


URL_CHARACTER_LIMIT = 2000  # number of characters
_EXTRA_CHARS_PER_ITEM = len("&field=")


class _WideTableFetcher:
    def __init__(self, get, link):
        self.get = get
        self.link = link
        self.variables = []
        self._dataframe = None

    def register(self, name, array_client, array_structure):
        if self._dataframe is not None:
            raise RuntimeError("Cannot add variables; already fetched.")
        self.variables.append(name)
        # TODO Can we avoid .values here?
        return dask.array.from_delayed(
            dask.delayed(self.dataframe)()[name].values,
            shape=array_structure.macro.shape,
            dtype=array_structure.micro.to_numpy_dtype(),
        )

    def dataframe(self):
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
            self._dataframe = pandas.concat(dataframes, axis=1)
        return self._dataframe.reset_index()

    def _fetch_variables(self, variables):
        content = self.get(
            self.link,
            params={"format": APACHE_ARROW_FILE_MIME_TYPE, "field": variables},
        )
        return deserialize_arrow(content)
