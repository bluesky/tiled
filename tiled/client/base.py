from ..utils import DictView, UNCHANGED
from .utils import get_json_with_cache


class BaseClientReader:
    def __init__(
        self,
        client,
        *,
        cache,
        offline,
        path,
        metadata,
        params,
        queries=None,
        containers=None,
        special_clients=None,
        root_client_type=None,
        structure=None,
    ):
        self._client = client
        self._offline = offline
        self._cache = cache
        self._metadata = metadata
        self._path = path
        self._params = params
        self._structure = structure

    def new_variation(
        self,
        class_,
        *,
        path=UNCHANGED,
        metadata=UNCHANGED,
        params=UNCHANGED,
        structure=UNCHANGED,
        containers=UNCHANGED,
        special_clients=UNCHANGED,
    ):
        """
        This is intended primarily for intenal use and use by subclasses.
        """
        if path is UNCHANGED:
            path = self._path
        if metadata is UNCHANGED:
            metadata = self._metadata
        if containers is UNCHANGED:
            containers = self.containers
        if special_clients is UNCHANGED:
            special_clients = self.special_clients
        if params is UNCHANGED:
            params = self._params
        if params is UNCHANGED:
            params = self._params
        return class_(
            client=self._client,
            offline=self._offline,
            cache=self._cache,
            path=path,
            metadata=metadata,
            params=params,
            structure=self._structure,
            containers=containers,
            special_clients=special_clients,
        )

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @property
    def metadata(self):
        "Metadata about this data source."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)


class BaseArrayClientReader(BaseClientReader):
    """
    Shared by Array, DataArray, Dataset

    Subclass must define:

    * STRUCTURE_TYPE : type
    """

    def structure(self):
        # Notice that we are NOT *caching* in self._structure here. We are
        # allowing that the creator of this instance might have already known
        # our structure (as part of the some larger structure) and passed it
        # in.
        if self._structure is None:
            content = get_json_with_cache(
                self._cache,
                self._offline,
                self._client,
                f"/metadata/{'/'.join(self._path)}",
                params={
                    "fields": ["structure.micro", "structure.macro"],
                    **self._params,
                },
            )
            result = content["data"]["attributes"]["structure"]
            structure = self.STRUCTURE_TYPE.from_json(result)
        else:
            structure = self._structure
        return structure
