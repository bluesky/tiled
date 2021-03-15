from ..utils import DictView
from .utils import handle_error


class BaseClientReader:
    """
    Subclass must define:

    * STRUCTURE_TYPE : type
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

    def structure(self):
        if self._structure is None:
            response = self._client.get(
                f"/metadata/{'/'.join(self._path)}",
                params={"fields": "structure", **self._params},
            )
            handle_error(response)
            result = response.json()["data"]["attributes"]["structure"]
            structure = self.STRUCTURE_TYPE.from_json(result)
        else:
            structure = self._structure
        return structure
