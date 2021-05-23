import mimetypes

from collections import defaultdict

from .utils import DictView


class Registry:
    """
    Registry of media types for each structure family

    Examples
    --------

    Register a JSON writer for "array" structures.
    (This is included by default but it is shown here as a simple example.)

    >>> import json
    >>>> serialization_registry.register(
        "array", "application/json", lambda array: json.dumps(array.tolist()).encode()
    )

    """

    def __init__(self):
        self._lookup = defaultdict(dict)
        # TODO Think about whether lazy registration makes any sense here.

    def media_types(self, structure_family):
        """
        List the supported media types for a given structure family.
        """
        return DictView(self._lookup[structure_family])

    @property
    def structure_families(self):
        """
        List the known structure families.
        """
        return list(self._lookup)

    def aliases(self, structure_family):
        """
        List the aliases (file extensions) for each media type for a given structure family.
        """
        result = {}
        for media_type in self.media_types(structure_family):
            if media_type == "application/octet-stream":
                # Skip the general binary type; it doesn't really apply.
                continue
            aliases = []
            for k, v in mimetypes.types_map.items():
                # e.g. k, v == (".csv", "text/csv")
                if v == media_type:
                    aliases.append(k[1:])  # e.g. aliases == {"text/csv": ["csv"]}
            if aliases:
                result[media_type] = aliases
        return result

    def register(self, structure_family, media_type, func):
        """
        Register a new media_type for a structure family.

        Parameters
        ----------
        structure_family : str
            The structure we are encoding, as in "array", "dataframe", "variable", ...
        media_type : str
            MIME type, as in "application/json" or "text/csv".
            If there is not standard name, use "application/x-INVENT-NAME-HERE".
        func : callable
            Should accept the relevant structure as input (e.g. a numpy array)
            and return bytes or memoryview
        """
        self._lookup[structure_family][media_type] = func

    def dispatch(self, structure_family, media_type):
        """
        Look up a writer for a given structure and media type.
        """
        try:
            return self._lookup[structure_family][media_type]
        except KeyError:
            pass
        raise ValueError(
            f"No dispatch for structure_family {structure_family} with media type {media_type}"
        )

    def __call__(self, structure_family, media_type, *args, **kwargs):
        """
        Invoke a writer for a given structure and media type.
        """
        return self.dispatch(structure_family, media_type)(*args, **kwargs)


serialization_registry = Registry()
"Global serialization registry. See Registry for usage examples."
# TODO Do we *need* a deserialization registry?
# The Python client always deals with a certain preferred format
# for each structure family. Deserializing other formats is other
# clients' problem, and we can't help with that from here.
deserialization_registry = Registry()
