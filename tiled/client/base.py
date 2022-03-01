import importlib

from ..utils import UNCHANGED, DictView, ListView, OneShotCachedMap
from .cache import Revalidate, verify_cache


class BaseClient:
    def __init__(self, context, *, item, path, params, structure_clients):
        self._context = context
        if isinstance(path, str):
            raise ValueError("path is expected to be a list of segments")
        # Stash *immutable* copies just to be safe.
        self._path = tuple(path or [])
        self._item = item
        self._cached_len = None  # a cache just for __len__
        self._params = params or {}
        self.structure_clients = structure_clients
        super().__init__()

    def login(self, provider=None):
        """
        Depending on the server's authentication method, this will prompt for username/password

        >>> c.login()
        Username: USERNAME
        Password: <input is hidden>

        or prompt you to open a link in a web browser to login with a third party and paste in a access code

        >>> c.login()
        Navigate your web browser to this address to obtain access code:

        ...

        Access code (quotes optional): <input is hidden>

        See also c.context.authenticate() and c.context.reauthenticate().
        """
        self.context.authenticate(provider=provider)
        # Do NOT return the tokens that are returned by authenticate().
        # This avoids displaying valid refresh tokens into places they might persist,
        # like Jupyter notebooks.

    def logout(self):
        """
        Log out.
        """
        self.context.logout()

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
        self, path=UNCHANGED, params=UNCHANGED, structure_clients=UNCHANGED, **kwargs
    ):
        """
        This is intended primarily for internal use and use by subclasses.
        """
        if path is UNCHANGED:
            path = self._path
        if params is UNCHANGED:
            params = self._params
        if structure_clients is UNCHANGED:
            structure_clients = self.structure_clients
        return type(self)(
            item=self._item,
            path=path,
            params=params,
            structure_clients=structure_clients,
            **kwargs,
        )


class BaseStructureClient(BaseClient):
    def __init__(self, *args, structure=None, **kwargs):
        super().__init__(*args, **kwargs)
        if structure is not None:
            # Allow the caller to optionally hand us a structure that is already
            # parsed from a dict into a structure dataclass.
            self._structure = structure
        else:
            attributes = self.item["attributes"]
            structure_type = STRUCTURE_TYPES[attributes["structure_family"]]
            self._structure = structure_type.from_json(attributes["structure"])

    def download(self):
        """
        Download all data into the cache.

        This causes it to be cached if the context is configured with a cache.
        """
        verify_cache(self.context.cache)
        repr(self)
        self.read()

    def refresh(self, force=False):
        """
        Refresh cached data for this node.

        Parameters
        ----------
        force: bool
            If False, (default) refresh only expired cache entries.
            If True, refresh all cache entries.
        """
        if force:
            revalidate = Revalidate.FORCE
        else:
            revalidate = Revalidate.IF_EXPIRED
        with self.context.revalidation(revalidate):
            self.download()

    def structure(self):
        """
        Return a dataclass describing the structure of the data.
        """
        if self._structure.macro.resizable:
            # In the future, conditionally fetch updated information.
            raise NotImplementedError(
                "The server has indicated that this has a dynamic, resizable "
                "structure and this version of the Tiled Python client cannot "
                "cope with that."
            )
        return self._structure


STRUCTURE_TYPES = OneShotCachedMap(
    {
        "array": lambda: importlib.import_module(
            "...structures.array", BaseStructureClient.__module__
        ).ArrayStructure,
        "dataframe": lambda: importlib.import_module(
            "...structures.dataframe", BaseStructureClient.__module__
        ).DataFrameStructure,
        "xarray_data_array": lambda: importlib.import_module(
            "...structures.xarray", BaseStructureClient.__module__
        ).DataArrayStructure,
        "xarray_dataset": lambda: importlib.import_module(
            "...structures.xarray", BaseStructureClient.__module__
        ).DatasetStructure,
    }
)
