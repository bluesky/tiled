from collections.abc import Iterable

import pandas
import xarray

from ..structures.array import ArrayStructure
from ..structures.dataframe import deserialize_arrow
from ..structures.xarray import (
    APACHE_ARROW_FILE_MIME_TYPE,
    DataArrayStructure,
    DatasetStructure,
)
from .array import ArrayClient, DaskArrayClient
from .base import BaseStructureClient
from .utils import export_util

LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION = 1_000_000


class DaskVariableClient(BaseStructureClient):

    STRUCTURE_TYPE = ArrayStructure  # used by base class
    ARRAY_CLIENT = DaskArrayClient  # overridden by subclass

    def _build_array_client(self):
        return self.ARRAY_CLIENT(
            context=self.context,
            item=self.item,
            path=self.path,
            params=self._params,
            structure_clients=self.structure_clients,
            structure=self.structure(),
        )

    @property
    def data(self):
        return self._build_array_client()

    def read_block(self, block, slice=None):
        """
        Read a block (optional sub-sliced) of array data from this Variable.

        Intended for advanced uses. Returns array-like, not Variable.
        """
        return self.data.read_block(block, slice)

    def read(self, slice=None):
        return xarray.Variable(
            dims=self.structure().macro.dims,
            data=self._build_array_client().read(slice),
            attrs=self.metadata,
        )

    def __getitem__(self, slice):
        return self.read(slice)

    # The default object.__iter__ works as expected here, no need to
    # implemented it specifically.

    def __len__(self):
        # As with numpy, len(arr) is the size of the zeroth axis.
        return self.structure().macro.shape[0]

    def download(self):
        super().download()
        self.read().compute()

    def export(self, filepath, format=None, slice=None, link=None, template_vars=None):
        """
        Download data in some format and write to a file.

        Parameters
        ----------
        file: str or buffer
            Filepath or writeable buffer.
        format : str, optional
            If format is None and `file` is a filepath, the format is inferred
            from the name, like 'table.csv' implies format="text/csv". The format
            may be given as a file extension ("csv") or a media type ("text/csv").
        slice : List[slice], optional
            List of slice objects. A convenient way to generate these is shown
            in the examples.
        link: str, optional
            Used internally. Refers to a key in the dictionary of links sent
            from the server.
        template_vars: dict, optional
            Used internally.

        Examples
        --------

        Export all.

        >>> a.export("numbers.csv")

        Export an N-dimensional slice.

        >>> import numpy
        >>> a.export("numbers.csv", slice=numpy.s_[:10, 50:100])
        """
        self._build_array_client().export(
            filepath, format=format, slice=slice, link=link, template_vars=template_vars
        )

    @property
    def formats(self):
        "List formats that the server can export this data as."
        return self.context.get_json("")["formats"]["variable"]


class VariableClient(DaskVariableClient):

    ARRAY_CLIENT = ArrayClient

    def download(self):
        # Do not run super().download() because DaskVariableClient calls compute()
        # which does not apply here.
        BaseStructureClient.download(self)
        self.read()


class DaskDataArrayClient(BaseStructureClient):

    STRUCTURE_TYPE = DataArrayStructure  # used by base class
    VARIABLE_CLIENT = DaskVariableClient  # overriden in subclass

    def __init__(
        self,
        *args,
        variable_name=None,
        coords=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._coords = coords
        self._variable_name = variable_name  # if this is contained by a DatasetClient

    def _build_variable_client(self, variable):
        item = {
            "attributes": {
                "metadata": self.metadata["attrs"],
                "structure_family": "array",
            },
            "links": {
                "self": self.item["links"]["self"] + "/variable",
                "full_variable": str(self.context.base_url)
                + "/array/full/"
                + "/".join(self.path)
                + "/variable",
            },
        }
        return self.VARIABLE_CLIENT(
            context=self.context,
            item=item,
            path=self.path + ["variable"],
            params=self._params,
            structure=variable,
            structure_clients=self.structure_clients,
        )

    def read_block(self, block, slice=None):
        """
        Read a block (optional sub-sliced) of array data from this DataArray's Variable.

        Intended for advanced uses. Returns array-like, not Variable.
        """
        variable = self.structure().macro.variable
        client = self._build_variable_client(variable)
        return client.read_block(block, slice)

    def _build_coords_clients(self):
        structure = self.structure().macro
        # If this is a DataArray within a Dataset or coord DataArray within a
        # DataArray, the coords are fetched once and passed in so that they are
        # not independently (re-)fetched by every DataArray.
        if self._coords is not None:
            coords_clients = {
                k: WrapperClient(v)
                for k, v in self._coords.items()
                if k in structure.coord_names
            }
            return coords_clients, self._coords
        coords_clients = {}
        coords = {}
        if structure.coords:
            for name, variable in structure.coords.items():
                item = {
                    "attributes": {"metadata": self.metadata["coords"][name]},
                    "links": {"self": self.item["links"]["self"] + f"/coords/{name}"},
                }
                client = DataArrayClient(
                    context=self.context,
                    item=item,
                    path=self.path + ["coords", name],
                    params=self._params,
                    structure=variable,
                    structure_clients=self.structure_clients,
                    coords=coords,
                )
                coords_clients[name] = client
        coords.update({k: v.read() for k, v in coords_clients.items()})
        return coords_clients, coords

    @property
    def coords(self):
        """
        A dict mapping coord names to Variables.

        Intended for advanced uses. Enables access to read_block(...) on coords.
        """
        coords_clients, _coords = self._build_coords_clients()
        return coords_clients

    def read(self, slice=None):
        if slice is None:
            slice = ()
        elif isinstance(slice, Iterable):
            slice = tuple(slice)
        else:
            slice = tuple([slice])
        structure = self.structure().macro
        client = self._build_variable_client(structure.variable)
        data = client.read(slice)
        # If this is part of a Dataset, the coords are fetched
        # once and passed in so that they are not independently
        # (re-)fetched by every DataArray.
        coords = {}
        for name, client in self.coords.items():
            coords[name] = client.read(slice)
        return xarray.DataArray(
            data=data,
            coords=coords,
            name=structure.name,
            attrs=self.item["attributes"]["metadata"]["attrs"],
        )

    def __getitem__(self, slice):
        return self.read(slice)

    # The default object.__iter__ works as expected here, no need to
    # implemented it specifically.

    def __len__(self):
        # As with numpy, len(arr) is the size of the zeroth axis.
        return self.structure().macro.variable.macro.data.macro.shape[0]

    def download(self):
        super().download()
        self.read().compute()

    def export_array(self, filepath, format=None, slice=None):
        """
        Download data in some format and write to a file.

        Parameters
        ----------
        file: str or buffer
            Filepath or writeable buffer.
        format : str, optional
            If format is None and `file` is a filepath, the format is inferred
            from the name, like 'table.csv' implies format="text/csv". The format
            may be given as a file extension ("csv") or a media type ("text/csv").
        slice : List[slice]
            List of slice objects. A convenient way to generate these is shown
            in the examples.

        Examples
        --------

        Export all.

        >>> a.export("numbers.csv")

        Export an N-dimensional slice.

        >>> import numpy
        >>> a.export("numbers.csv", slice=numpy.s_[:10, 50:100])
        """
        variable = self.structure().macro.variable
        template_vars = {}
        if self._variable_name is not None:
            # This is a stand-alone DataArray.
            template_vars.update({"variable": self._variable_name})
        self._build_variable_client(variable).export(
            filepath,
            format=format,
            slice=slice,
            link="full_variable",
            template_vars=template_vars,
        )

    def export_all(self, filepath, format=None, slice=None):
        """
        Export data and coords.
        """
        # The server has no endpoint for this.
        # It's not clear if there any any appropriate formats for it.
        # Export the whole Dataset instead.
        raise NotImplementedError


class DataArrayClient(DaskDataArrayClient):

    VARIABLE_CLIENT = VariableClient

    def download(self):
        # Do not run super().download() because DaskDataArrayClient calls compute()
        # which does not apply here.
        BaseStructureClient.download(self)
        self.read()


class DaskDatasetClient(BaseStructureClient):

    STRUCTURE_TYPE = DatasetStructure  # used by base class
    DATA_ARRAY_CLIENT = DaskDataArrayClient  # overridden by subclass
    VARIABLE_CLIENT = DaskVariableClient  # overridden by subclass

    def _repr_pretty_(self, p, cycle):
        """
        Provide "pretty" display in IPython/Jupyter.

        See https://ipython.readthedocs.io/en/stable/config/integrating.html#rich-display
        """
        structure = self.structure()
        if not structure.macro.resizable:
            # Use cached structure.
            variables = [*structure.macro.data_vars, *structure.macro.coords]
            p.text(f"<{type(self).__name__} {variables}>")
        else:
            # Try to get the column names, but give up quickly to avoid blocking
            # for long.
            TIMEOUT = 0.2  # seconds
            try:
                content = self.context.get_json(
                    self.uri,
                    params={"fields": "structure.macro", **self._params},
                    timeout=TIMEOUT,
                )
            except TimeoutError:
                p.text(
                    f"<{type(self).__name__} Loading column names took too long; use list(...) >"
                )
            except Exception as err:
                p.text(
                    f"<{type(self).__name__} Loading column names raised error {err!r}>"
                )
            else:
                try:
                    macro = content["data"]["attributes"]["structure"]["macro"]
                    variables = [*macro["data_vars"], *macro["coords"]]
                except Exception as err:
                    p.text(
                        f"<{type(self).__name__} Loading column names raised error {err!r}>"
                    )
                else:
                    p.text(f"<{type(self).__name__} {variables}>")

    def _ipython_key_completions_(self):
        """
        Provide method for the key-autocompletions in IPython.

        See http://ipython.readthedocs.io/en/stable/config/integrating.html#tab-completion
        """
        structure = self.structure()
        if not structure.macro.resizable:
            # Use cached structure.
            variables = [*structure.macro.data_vars, *structure.macro.coords]
        else:
            try:
                content = self.context.get_json(
                    self.uri, params={"fields": "structure.macro", **self._params}
                )
                macro = content["data"]["attributes"]["structure"]["macro"]
                variables = [*macro["data_vars"], *macro["coords"]]
            except Exception:
                # Do not print messy traceback from thread. Just fail silently.
                return []
        return variables

    def download(self):
        super().download()
        self._ipython_key_completions_()
        self.read().compute()

    @property
    def data_vars(self):
        structure = self.structure().macro
        _coords_clients, coords = self._build_coords_clients(structure)
        # Turn off wide table optimization so that we return normal
        # [Dask]DataArrayClients and not internal _MockClients.
        return self._build_data_vars_clients(
            structure, coords, optimize_wide_table=False
        )

    @property
    def coords(self):
        structure = self.structure().macro
        coords_clients, _coords = self._build_coords_clients(structure)
        return coords_clients

    def _build_data_vars_clients(
        self, structure, coords, variables=None, optimize_wide_table=True
    ):
        data_vars_clients = {}
        wide_table_fetcher = _WideTableFetcher(
            self.context.get_content, self.item["links"]["full_dataset"], coords
        )
        for name, data_array in structure.data_vars.items():
            if (variables is not None) and (name not in variables):
                continue

            metadata = self.metadata["data_vars"][name]
            # Optimization: Download scalar data as DataFrame.
            data_shape = data_array.macro.variable.macro.shape
            if (
                optimize_wide_table
                and (data_shape[0] < LENGTH_LIMIT_FOR_WIDE_TABLE_OPTIMIZATION)
                and (len(data_shape) < 2)
            ):
                data_vars_clients[name] = wide_table_fetcher.register(
                    name, data_array, metadata
                )
            else:
                item = {
                    "attributes": {"metadata": metadata},
                    "links": {
                        "self": self.item["links"]["self"]
                        + f"/data_vars/{name}/variable"
                    },
                }
                client = self.DATA_ARRAY_CLIENT(
                    context=self.context,
                    item=item,
                    path=self.path + ["data_vars", name],
                    params=self._params,
                    structure=data_array,
                    coords=coords,
                    variable_name=name,
                    structure_clients=self.structure_clients,
                )
                data_vars_clients[name] = client
        return data_vars_clients

    def _build_coords_clients(self, structure):
        coords_clients = {}
        coords = {}
        for name, variable in structure.coords.items():
            # Xarray greedily materializes coordiantes; they are not allowed to
            # remain dask arrays. If we use self.DATA_ARRAY_CLIENT here, which
            # may be DaskDataArrayClient, then each DataArray separately calls
            # compute() and redundantly issues the same request. To avoid that,
            # we fetch greedily here.
            item = {
                "attributes": {"metadata": self.metadata["coords"][name]},
                "links": {
                    "self": self.item["links"]["self"] + f"/coords/{name}/variable"
                },
            }
            client = DataArrayClient(
                context=self.context,
                item=item,
                path=self.path + ["coords", name],
                params=self._params,
                structure=variable,
                structure_clients=self.structure_clients,
                variable_name=name,
                coords=coords,
            )
            coords_clients[name] = client
        coords.update({k: v.read() for k, v in coords_clients.items()})
        return coords_clients, coords

    def read(self, variables=None):
        structure = self.structure().macro
        _coords_clients, coords = self._build_coords_clients(structure)
        data_vars_clients = self._build_data_vars_clients(structure, coords, variables)
        data_vars = {k: v.read() for k, v in data_vars_clients.items()}
        ds = xarray.Dataset(
            data_vars=data_vars, coords=coords, attrs=self.metadata["attrs"]
        )
        return ds

    def __getitem__(self, variables):
        # This is type unstable, matching xarray's behavior.
        if isinstance(variables, str):
            # Return a single column (an xarray.DataArray).
            return self.read(variables=[variables])[variables]
        else:
            # Return an xarray.Dataset with a subset of the available variables.
            return self.read(variables=variables)

    def __iter__(self):
        # This reflects a slight weirdness in xarray, where coordinates can be
        # used in __getitem__ and __contains__, as in `ds[coord_name]` and
        # `coord_name in ds`, but they are not included in the result of
        # `list(ds)`.
        yield from self.structure().macro.data_vars

    def export(self, filepath, format=None, variables=None):
        """
        Download data in some format and write to a file.

        Parameters
        ----------
        file: str or buffer
            Filepath or writeable buffer.
        format : str, optional
            If format is None and `file` is a filepath, the format is inferred
            from the name, like 'table.csv' implies format="text/csv". The format
            may be given as a file extension ("csv") or a media type ("text/csv").
        variables: List[str], optional
        """
        params = {}
        if variables is not None:
            # Note: The singular/plural inconsistency here is due to the fact that
            # ["A", "B"] will be encoded in the URL as field=A&field=B
            params["field"] = variables
        return export_util(
            filepath,
            format,
            self.context.get_content,
            self.item["links"]["full_dataset"],
            params=params,
        )

    @property
    def formats(self):
        "List formats that the server can export this data as."
        return self.context.get_json("")["formats"]["xarray_dataset"]


class DatasetClient(DaskDatasetClient):

    DATA_ARRAY_CLIENT = DataArrayClient
    VARIABLE_CLIENT = VariableClient

    def download(self):
        # Do not run super().download() because DaskDatasetClient calls compute()
        # which does not apply here.
        BaseStructureClient.download(self)
        self._ipython_key_completions_()
        self.read()


URL_CHARACTER_LIMIT = 2000  # number of characters
_EXTRA_CHARS_PER_ITEM = len("&field=")


class _WideTableFetcher:
    def __init__(self, get, link, coords):
        self.get = get
        self.link = link
        self.coords = coords
        self.variables = []
        self._dataframe = None

    def register(self, name, data_array, metadata):
        if self._dataframe is not None:
            raise RuntimeError("Cannot add variables; already fetched.")
        self.variables.append(name)
        return _MockClient(self, name, data_array, metadata)

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
        return self._dataframe

    def _fetch_variables(self, variables):
        content = self.get(
            self.link,
            params={"format": APACHE_ARROW_FILE_MIME_TYPE, "field": variables},
        )
        return deserialize_arrow(content)


class _MockClient:
    def __init__(self, wto, name, data_array_structure, metadata):
        self.wto = wto
        self.name = name
        self.data_array_structure = data_array_structure
        self.metadata = metadata

    def read(self):
        # TODO Can we avoid .values here?
        data = self.wto.dataframe()[self.name].values
        s = self.data_array_structure
        variable = xarray.Variable(
            data=data,
            dims=s.macro.variable.macro.dims,
            # attrs=s.macro.variable.macro.attrs,
        )
        coords = {name: self.wto.coords[name] for name in s.macro.coord_names}
        return xarray.DataArray(
            variable, name=s.macro.name, coords=coords, attrs=self.metadata["attrs"]
        )


class WrapperClient:
    # TODO Build out the rest of the client API.
    def __init__(self, data_array):
        self._data_array = data_array

    def read(self, slice=None):
        if slice is not None:
            result = self._data_array[slice]
        else:
            result = self._data_array
        return result
