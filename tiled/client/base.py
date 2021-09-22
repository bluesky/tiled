import importlib

from ..utils import DictView, ListView, OneShotCachedMap
from ..trees.utils import UNCHANGED


class BaseClient:
    def __init__(
        self,
        context,
        *,
        item,
        path,
        params,
    ):
        self._context = context
        if isinstance(path, str):
            raise ValueError("path is expected to be a list of segments")
        # Stash *immutable* copies just to be safe.
        self._path = tuple(path or [])
        self._item = item
        self._cached_len = None  # a cache just for __len__
        self._params = params or {}
        super().__init__()

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @property
    def context(self):
        return self._context

    @property
    def item(self):
        "JSON payload describing this item. Mostly for internal use."
        return self._item

    @property
    def metadata(self):
        "Metadata about this data source."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._item["attributes"]["metadata"])

    @property
    def path(self):
        "Sequence of entry names from the root Tree to this entry"
        return ListView(self._path)

    @property
    def uri(self):
        "Direct link to this entry"
        return self.item["links"]["self"]

    @property
    def username(self):
        return self.context.username

    @property
    def offline(self):
        return self.context.offline

    @offline.setter
    def offline(self, value):
        self.context.offline = bool(value)

    def new_variation(
        self,
        path=UNCHANGED,
        params=UNCHANGED,
        **kwargs,
    ):
        """
        This is intended primarily for internal use and use by subclasses.
        """
        if path is UNCHANGED:
            path = self._path
        if params is UNCHANGED:
            params = self._params
        return type(self)(
            item=self._item,
            path=path,
            params=params,
            **kwargs,
        )


class BaseStructureClient(BaseClient):
    def __init__(
        self,
        context,
        *,
        structure=None,
        **kwargs,
    ):
        super().__init__(context, **kwargs)
        self._structure = structure

    def new_variation(self, structure=UNCHANGED, **kwargs):
        if structure is UNCHANGED:
            structure = self._structure
        return super().new_variation(structure=structure, **kwargs)

    def touch(self):
        """
        Access all the data.

        This causes it to be cached if the context is configured with a cache.
        """
        repr(self)
        self.read()

    def structure(self):
        """
        Return a dataclass describing the structure of the data.
        """
        # This is implemented by subclasses.
        pass


class BaseArrayClient(BaseStructureClient):
    """
    Shared by Array, DataArray, Dataset
    """

    def __init__(self, *args, route, **kwargs):
        if route.endswith("/"):
            route = route[:-1]
        self._route = route
        super().__init__(*args, **kwargs)

    def structure(self):
        # Notice that we are NOT *caching* in self._structure here. We are
        # allowing that the creator of this instance might have already known
        # our structure (as part of the some larger structure) and passed it
        # in.
        if self._structure is None:
            content = self.context.get_json(
                self.uri,
                params={
                    "fields": [
                        "structure.micro",
                        "structure.macro",
                        "structure_family",
                    ],
                    **self._params,
                },
            )
            attributes = content["data"]["attributes"]
            structure_type = ARRAY_STRUCTURE_TYPES[attributes["structure_family"]]
            structure = structure_type.from_json(attributes["structure"])
        else:
            structure = self._structure
        return structure


# Defer imports to avoid numpy requirement in this module.
ARRAY_STRUCTURE_TYPES = OneShotCachedMap(
    {
        "array": lambda: importlib.import_module(
            "...structures.array", BaseArrayClient.__module__
        ).ArrayStructure,
        "structured_array_generic": lambda: importlib.import_module(
            "...structures.structured_array", BaseArrayClient.__module__
        ).StructuredArrayGenericStructure,
        "structured_array_tabular": lambda: importlib.import_module(
            "...structures.structured_array", BaseArrayClient.__module__
        ).StructuredArrayTabularStructure,
        "variable": lambda: importlib.import_module(
            "...structures.xarray", BaseArrayClient.__module__
        ).VariableStructure,
        "data_array": lambda: importlib.import_module(
            "...structures.xarray", BaseArrayClient.__module__
        ).DataArrayStructure,
        "dataset": lambda: importlib.import_module(
            "...structures.xarray", BaseArrayClient.__module__
        ).DatasetStructure,
    }
)
