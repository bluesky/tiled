from ..utils import DictView
from .utils import handle_error


class BaseClientReader:
    """
    Subclass must define:

    * read()
    """

    def __init__(
        self,
        client,
        *,
        path,
        metadata,
        params,
        containers=None,
        special_clients=None,
        root_client_type=None,
        structure=None,
    ):
        self._client = client
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


class BaseArrayClientReader(BaseClientReader):
    """
    Shared by Array, DataArray, Dataset

    Subclass must define:

    * MICROSTRUCTURE_TYPE : type
    * MACROSTRUCTURE_TYPE : type
    * STRUCTURE_TYPE : type
    """

    def structure(self):
        # Notice that we are NOT *caching* in self._structure here. We are
        # allowing that the creator of this instance might have already known
        # our structure (as part of the some larger structure) and passed it
        # in.
        if self._structure is None:
            response = self._client.get(
                f"/metadata/{'/'.join(self._path)}",
                params={
                    "fields": ["structure.micro", "structure.macro"],
                    **self._params,
                },
            )
            handle_error(response)
            result = response.json()["data"]["attributes"]["structure"]
            structure = {}
            structure["macro"] = self.MACROSTRUCTURE_TYPE.from_json(result["macro"])
            if self.MICROSTRUCTURE_TYPE is not None:
                # xarrays have not microstructure
                structure["micro"] = self.MICROSTRUCTURE_TYPE.from_json(result["micro"])
            else:
                structure["micro"] = None
            structure_ = self.STRUCTURE_TYPE(**structure)
        else:
            structure_ = self._structure
        return structure_
