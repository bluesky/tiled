import importlib
import time

from ..utils import UNCHANGED, DictView, ListView, OneShotCachedMap
from .cache import Revalidate, verify_cache


class MetadataRevisions:
    def __init__(self, context, path):
        self._cached_len = None
        self.context = context
        self._path = path

    def __len__(self):

        LENGTH_CACHE_TTL = 1  # second

        now = time.monotonic()
        if self._cached_len is not None:
            length, deadline = self._cached_len
            if now < deadline:
                # Used the cached value and do not make any request.
                return length

        path = (
            "/node/revisions"
            + "".join(f"/{part}" for part in self.context.path_parts)
            + "".join(f"/{part}" for part in (self._path or [""]))
        )

        content = self.context.get_json(
            path, params={"page[offset]": 0, "page[limit]": 0}
        )
        length = content["meta"]["count"]
        self._cached_len = (length, now + LENGTH_CACHE_TTL)
        return length

    def __getitem__(self, item_):
        self._cached_len = None

        path = (
            "/node/revisions"
            + "".join(f"/{part}" for part in self.context.path_parts)
            + "".join(f"/{part}" for part in (self._path or [""]))
        )

        if isinstance(item_, int):
            offset = item_
            limit = 1

            content = self.context.get_json(
                path, params={"page[offset]": offset, "page[limit]": limit}
            )

            (result,) = content["data"]
            return result

        elif isinstance(item_, slice):
            offset = item_.start
            if offset is None:
                offset = 0
            if item_.stop is None:
                params = f"?page[offset]={offset}"
            else:
                limit = item_.stop - offset
                params = f"?page[offset]={offset}&page[limit]={limit}"

            next_page = path + params
            result = []
            while next_page is not None:
                content = self.context.get_json(next_page)
                if len(result) == 0:
                    result = content.copy()
                else:
                    result["data"].append(content["data"])
                next_page = content["links"]["next"]

            return result["data"]

    def delete_revision(self, n):

        path = (
            "/node/revisions"
            + "".join(f"/{part}" for part in self.context.path_parts)
            + "".join(f"/{part}" for part in (self._path or [""]))
        )

        self.context.delete_content(path, None, params={"n": n})


class BaseClient:
    def __init__(self, context, *, item, path, structure_clients):
        self._context = context
        if isinstance(path, str):
            raise ValueError("path is expected to be a list of segments")
        # Stash *immutable* copies just to be safe.
        self._path = tuple(path or [])
        self._item = item
        self._cached_len = None  # a cache just for __len__
        self.structure_clients = structure_clients
        self._metadata_revisions = None
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
    def specs(self):
        "List of specifications describing the structure of the metadata and/or data."
        return ListView(self.item["attributes"]["specs"])

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

    def new_variation(self, path=UNCHANGED, structure_clients=UNCHANGED, **kwargs):
        """
        This is intended primarily for internal use and use by subclasses.
        """
        if path is UNCHANGED:
            path = self._path
        if structure_clients is UNCHANGED:
            structure_clients = self.structure_clients
        return type(self)(
            item=self._item,
            path=path,
            structure_clients=structure_clients,
            **kwargs,
        )

    @property
    def formats(self):
        "List formats that the server can export this data as."
        formats = set()
        for spec in self.item["attributes"]["specs"]:
            formats.update(self.context.get_json("")["formats"].get(spec, []))
        formats.update(
            self.context.get_json("")["formats"][
                self.item["attributes"]["structure_family"]
            ]
        )
        return sorted(formats)

    def update_metadata(self, metadata=None, specs=None):
        """
        EXPERIMENTAL: Update metadata.

        This is subject to change or removal without notice

        Parameters
        ----------
        metadata : dict, optional
            User metadata. May be nested. Must contain only basic types
            (e.g. numbers, strings, lists, dicts) that are JSON-serializable.
        specs : List[str], optional
            List of names that are used to label that the data and/or metadata
            conform to some named standard specification.
        """

        self._cached_len = None

        data = {
            "metadata": metadata,
            "specs": specs,
        }

        full_path_meta = (
            "/node/metadata"
            + "".join(f"/{part}" for part in self.context.path_parts)
            + "".join(f"/{part}" for part in (self._path or [""]))
        )

        content = self.context.put_json(full_path_meta, data)

        if metadata is not None:
            if "metadata" in content:
                # Metadata was accepted and modified by the specs validator on the server side.
                # It is updated locally using the new version.
                self._item["attributes"]["metadata"] = content["metadata"]
            else:
                # Metadata was accepted as it si by the server.
                # It is updated locally with the version submitted buy the client.
                self._item["attributes"]["metadata"] = metadata

        if specs is not None:
            self._item["attributes"]["specs"] = specs

    @property
    def metadata_revisions(self):
        if self._metadata_revisions is None:
            self._metadata_revisions = MetadataRevisions(self.context, self._path)

        return self._metadata_revisions


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
        if getattr(getattr(self._structure, "macro", None), "resizable", None):
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
        "sparse": lambda: importlib.import_module(
            "...structures.sparse", BaseStructureClient.__module__
        ).SparseStructure,
    }
)
