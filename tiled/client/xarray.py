from collections.abc import Iterable
import builtins
import itertools

import xarray

from ..structures.xarray import (
    DataArrayStructure,
    DatasetStructure,
    VariableStructure,
)

from .array import ClientArrayAdapter, ClientDaskArrayAdapter
from .base import BaseArrayClientReader
from .utils import get_json_with_cache


class ClientDaskVariableAdapter(BaseArrayClientReader):

    STRUCTURE_TYPE = VariableStructure  # used by base class
    ARRAY_READER = ClientDaskArrayAdapter  # overridden by subclass

    def __init__(self, *args, route="/variable/block", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def _build_array_reader(self, structure):
        return self.ARRAY_READER(
            client=self._client,
            offline=self._offline,
            cache=self._cache,
            path=self._path,
            metadata=self.metadata,
            params=self._params,
            structure=structure.data,
            route=self._route,
        )

    @property
    def data(self):
        return self._build_array_reader(self.structure().macro)

    def read_block(self, block, slice=None):
        """
        Read a block (optional sub-sliced) of array data from this Variable.

        Intended for advanced uses. Returns array-like, not Variable.
        """
        return self.data.read_block(block, slice)

    def read(self, slice=None):
        structure = self.structure().macro
        return xarray.Variable(
            dims=structure.dims,
            data=self._build_array_reader(structure).read(slice),
            attrs=structure.attrs,
        )

    def __getitem__(self, slice):
        return self.read(slice)

    # The default object.__iter__ works as expected here, no need to
    # implemented it specifically.

    def __len__(self):
        # As with numpy, len(arr) is the size of the zeroth axis.
        return self.structure().macro.data.macro.shape[0]

    def touch(self):
        super().touch()
        self.read().compute()


class ClientVariableAdapter(ClientDaskVariableAdapter):

    ARRAY_READER = ClientArrayAdapter


class ClientDaskDataArrayAdapter(BaseArrayClientReader):

    STRUCTURE_TYPE = DataArrayStructure  # used by base class
    VARIABLE_READER = ClientDaskVariableAdapter  # overriden in subclass

    def __init__(self, *args, route="/data_array/block", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def read_block(self, block, slice=None):
        """
        Read a block (optional sub-sliced) of array data from this DataArray's Variable.

        Intended for advanced uses. Returns array-like, not Variable.
        """
        structure = self.structure().macro
        variable = structure.variable
        variable_source = self.VARIABLE_READER(
            client=self._client,
            offline=self._offline,
            cache=self._cache,
            path=self._path,
            metadata=self.metadata,
            params=self._params,
            structure=variable,
            route=self._route,
        )
        return variable_source.read_block(block, slice)

    @property
    def coords(self):
        """
        A dict mapping coord names to Variables.

        Intended for advanced uses. Enables access to read_block(...) on coords.
        """
        structure = self.structure().macro
        result = {}
        for name, variable in structure.coords.items():
            variable_source = self.VARIABLE_READER(
                client=self._client,
                offline=self._offline,
                cache=self._cache,
                path=self._path,
                metadata=self.metadata,
                params={"coord": name, **self._params},
                structure=variable,
                route=self._route,
            )
            result[name] = variable_source
        return result

    def read(self, slice=None):
        if slice is None:
            slice = ()
        elif isinstance(slice, Iterable):
            slice = tuple(slice)
        else:
            slice = tuple([slice])
        structure = self.structure().macro
        variable = structure.variable
        variable_source = self.VARIABLE_READER(
            client=self._client,
            offline=self._offline,
            cache=self._cache,
            path=self._path,
            metadata=self.metadata,
            params=self._params,
            structure=variable,
            route=self._route,
        )
        data = variable_source.read(slice)
        coords = {}
        for coord_slice, (name, variable) in itertools.zip_longest(
            slice, structure.coords.items(), fillvalue=builtins.slice(None, None)
        ):
            variable_source = self.VARIABLE_READER(
                client=self._client,
                offline=self._offline,
                cache=self._cache,
                path=self._path,
                metadata=self.metadata,
                params={"coord": name, **self._params},
                structure=variable,
                route=self._route,
            )
            coords[name] = variable_source.read(coord_slice)
        return xarray.DataArray(data=data, coords=coords, name=structure.name)

    def __getitem__(self, slice):
        return self.read(slice)

    # The default object.__iter__ works as expected here, no need to
    # implemented it specifically.

    def __len__(self):
        # As with numpy, len(arr) is the size of the zeroth axis.
        return self.structure().macro.variable.macro.data.macro.shape[0]

    def touch(self):
        super().touch()
        self.read().compute()


class ClientDataArrayAdapter(ClientDaskDataArrayAdapter):

    VARIABLE_READER = ClientVariableAdapter


class ClientDaskDatasetAdapter(BaseArrayClientReader):

    STRUCTURE_TYPE = DatasetStructure  # used by base class
    DATA_ARRAY_READER = ClientDaskDataArrayAdapter  # overridden by subclass
    VARIABLE_READER = ClientDaskVariableAdapter  # overridden by subclass

    def __init__(self, *args, route="/dataset/block", **kwargs):
        super().__init__(*args, **kwargs)
        self._route = route

    def _repr_pretty_(self, p, cycle):
        """
        Provide "pretty" display in IPython/Jupyter.

        See https://ipython.readthedocs.io/en/stable/config/integrating.html#rich-display
        """
        # Try to get the column names, but give up quickly to avoid blocking
        # for long.
        TIMEOUT = 0.2  # seconds
        try:
            content = get_json_with_cache(
                self._cache,
                self._offline,
                self._client,
                f"/metadata/{'/'.join(self._path)}",
                params={"fields": "structure.macro", **self._params},
                timeout=TIMEOUT,
            )
        except TimeoutError:
            p.text(
                f"<{type(self).__name__} Loading column names took too long; use list(...) >"
            )
        except Exception as err:
            p.text(f"<{type(self).__name__} Loading column names raised error {err!r}>")
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
        try:
            content = get_json_with_cache(
                self._cache,
                self._offline,
                self._client,
                f"/metadata/{'/'.join(self._path)}",
                params={"fields": "structure.macro", **self._params},
            )
            macro = content["data"]["attributes"]["structure"]["macro"]
            variables = [*macro["data_vars"], *macro["coords"]]
        except Exception:
            # Do not print messy traceback from thread. Just fail silently.
            return []
        return variables

    def touch(self):
        super().touch()
        self._ipython_key_completions_()
        self.read().compute()

    @property
    def data_vars(self):
        structure = self.structure().macro
        return self._build_data_vars(structure)

    @property
    def coords(self):
        structure = self.structure().macro
        return self._build_coords(structure)

    def _build_data_vars(self, structure, variables=None):
        data_vars = {}
        for name, data_array in structure.data_vars.items():
            if (variables is not None) and (name not in variables):
                continue
            data_array_source = self.DATA_ARRAY_READER(
                client=self._client,
                offline=self._offline,
                cache=self._cache,
                path=self._path,
                metadata=self.metadata,
                params={"variable": name, **self._params},
                structure=data_array,
                route=self._route,
            )
            data_vars[name] = data_array_source
        return data_vars

    def _build_coords(self, structure, variables=None):
        coords = {}
        for name, variable in structure.coords.items():
            if (variables is not None) and (name not in variables):
                continue
            variable_source = self.VARIABLE_READER(
                client=self._client,
                offline=self._offline,
                cache=self._cache,
                path=self._path,
                metadata=self.metadata,
                params={"variable": name, **self._params},
                structure=variable,
                route=self._route,
            )
            coords[name] = variable_source
        return coords

    def read(self, variables=None):
        structure = self.structure().macro
        data_vars = self._build_data_vars(structure, variables)
        coords = self._build_coords(structure, variables)
        return xarray.Dataset(
            data_vars={k: v.read() for k, v in data_vars.items()},
            coords={k: v.read() for k, v in coords.items()},
            attrs=structure.attrs,
        )

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


class ClientDatasetAdapter(ClientDaskDatasetAdapter):

    DATA_ARRAY_READER = ClientDataArrayAdapter
    VARIABLE_READER = ClientVariableAdapter
