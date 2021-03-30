from ..utils import DictView
from .utils import get_json_with_cache
from ..catalogs.utils import UNCHANGED


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
        structure=None,
    ):
        self._client = client
        self._offline = offline
        self._cache = cache
        self._metadata = metadata
        self._path = path
        self._params = params
        self._structure = structure

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @property
    def metadata(self):
        "Metadata about this data source."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def new_variation(
        self,
        offline=UNCHANGED,
        cache=UNCHANGED,
        metadata=UNCHANGED,
        path=UNCHANGED,
        params=UNCHANGED,
        structure=UNCHANGED,
    ):
        """
        This is intended primarily for intenal use and use by subclasses.
        """
        if offline is UNCHANGED:
            self._offline = offline
        if cache is UNCHANGED:
            self._cache = offline
        if metadata is UNCHANGED:
            self._metadata = metadata
        if path is UNCHANGED:
            self._path = path
        if params is UNCHANGED:
            self._params = params
        if structure is UNCHANGED:
            self._structure = structure
        return type(self)(
            client=self._client,
            offline=offline,
            cache=cache,
            metadata=metadata,
            path=path,
            params=params,
            structure=structure,
        )

    def touch(self):
        """
        Access all the data from the Reader.

        This causes it to be cached if the client is configured with a cache.
        """
        repr(self)
        self.read()


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
