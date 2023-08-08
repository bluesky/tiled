import importlib
import time
from dataclasses import asdict

from ..structures.core import Spec, StructureFamily
from ..utils import UNCHANGED, DictView, ListView, OneShotCachedMap, safe_json_dump
from .utils import MSGPACK_MIME_TYPE, handle_error


class MetadataRevisions:
    def __init__(self, context, link):
        self._cached_len = None
        self.context = context
        self._link = link

    def __len__(self):
        LENGTH_CACHE_TTL = 1  # second

        now = time.monotonic()
        if self._cached_len is not None:
            length, deadline = self._cached_len
            if now < deadline:
                # Used the cached value and do not make any request.
                return length

        content = handle_error(
            self.context.http_client.get(
                self._link,
                headers={"Accept": MSGPACK_MIME_TYPE},
                params={"page[offset]": 0, "page[limit]": 0},
            )
        ).json()
        length = content["meta"]["count"]
        self._cached_len = (length, now + LENGTH_CACHE_TTL)
        return length

    def __getitem__(self, item_):
        self._cached_len = None

        if isinstance(item_, int):
            offset = item_
            limit = 1

            content = handle_error(
                self.context.http_client.get(
                    self._link,
                    headers={"Accept": MSGPACK_MIME_TYPE},
                    params={"page[offset]": offset, "page[limit]": limit},
                )
            ).json()
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

            next_page = self._link + params
            result = []
            while next_page is not None:
                content = handle_error(
                    self.context.http_client.get(
                        next_page,
                        headers={"Accept": MSGPACK_MIME_TYPE},
                    )
                ).json()
                if len(result) == 0:
                    result = content.copy()
                else:
                    result["data"].append(content["data"])
                next_page = content["links"]["next"]

            return result["data"]

    def delete_revision(self, n):
        handle_error(self.context.http_client.delete(self._link, params={"number": n}))


class BaseClient:
    def __init__(self, context, *, item, structure_clients, structure=None):
        self._context = context
        self._item = item
        self._cached_len = None  # a cache just for __len__
        self.structure_clients = structure_clients
        self._metadata_revisions = None
        attributes = self.item["attributes"]
        structure_family = attributes["structure_family"]
        if structure is not None:
            # Allow the caller to optionally hand us a structure that is already
            # parsed from a dict into a structure dataclass.
            self._structure = structure
        elif structure_family == StructureFamily.container:
            self._structure = None
        else:
            structure_type = STRUCTURE_TYPES[attributes["structure_family"]]
            self._structure = structure_type.from_json(attributes["structure"])
        super().__init__()

    def structure(self):
        """
        Return a dataclass describing the structure of the data.
        """
        if getattr(self._structure, "resizable", None):
            # In the future, conditionally fetch updated information.
            raise NotImplementedError(
                "The server has indicated that this has a dynamic, resizable "
                "structure and this version of the Tiled Python client cannot "
                "cope with that."
            )
        return self._structure

    def login(self, username=None, provider=None):
        """
        Depending on the server's authentication method, this will prompt for username/password:

        >>> c.login()
        Username: USERNAME
        Password: <input is hidden>

        or prompt you to open a link in a web browser to login with a third party:

        >>> c.login()
        You have ... minutes visit this URL

        https://...

        and enter the code: XXXX-XXXX
        """
        self.context.login(username=username, provider=provider)

    def logout(self):
        """
        Log out.

        This method is idempotent: if you are already logged out, it will do nothing.
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
        return ListView([Spec(**spec) for spec in self._item["attributes"]["specs"]])

    @property
    def uri(self):
        "Direct link to this entry"
        return self.item["links"]["self"]

    def new_variation(self, structure_clients=UNCHANGED, **kwargs):
        """
        This is intended primarily for internal use and use by subclasses.
        """
        if structure_clients is UNCHANGED:
            structure_clients = self.structure_clients
        return type(self)(
            item=self._item,
            structure_clients=structure_clients,
            **kwargs,
        )

    @property
    def formats(self):
        "List formats that the server can export this data as."
        formats = set()
        for spec in self.item["attributes"]["specs"]:
            formats.update(self.context.server_info["formats"].get(spec, []))
        formats.update(
            self.context.server_info["formats"][
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

        if specs is None:
            normalized_specs = None
        else:
            normalized_specs = []
            for spec in specs:
                if isinstance(spec, str):
                    spec = Spec(spec)
                normalized_specs.append(asdict(spec))
        data = {
            "metadata": metadata,
            "specs": normalized_specs,
        }

        content = handle_error(
            self.context.http_client.put(
                self.item["links"]["self"], content=safe_json_dump(data)
            )
        ).json()

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
            self._item["attributes"]["specs"] = normalized_specs

    @property
    def metadata_revisions(self):
        if self._metadata_revisions is None:
            link = self.item["links"]["self"].replace("/metadata", "/revisions", 1)
            self._metadata_revisions = MetadataRevisions(self.context, link)

        return self._metadata_revisions


STRUCTURE_TYPES = OneShotCachedMap(
    {
        StructureFamily.array: lambda: importlib.import_module(
            "...structures.array", BaseClient.__module__
        ).ArrayStructure,
        StructureFamily.awkward: lambda: importlib.import_module(
            "...structures.awkward", BaseClient.__module__
        ).AwkwardStructure,
        StructureFamily.table: lambda: importlib.import_module(
            "...structures.table", BaseClient.__module__
        ).TableStructure,
        StructureFamily.sparse: lambda: importlib.import_module(
            "...structures.sparse", BaseClient.__module__
        ).SparseStructure,
    }
)
