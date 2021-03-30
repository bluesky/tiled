import mimetypes

from collections import defaultdict

from .utils import DictView


class Registry:
    def __init__(self):
        self._lookup = defaultdict(dict)
        # TODO Think about whether lazy registration makes any sense here.

    def media_types(self, structure_family):
        return DictView(self._lookup[structure_family])

    @property
    def structure_families(self):
        return list(self._lookup)

    def aliases(self, structure_family):
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
        self._lookup[structure_family][media_type] = func

    def dispatch(self, structure_family, media_type):
        try:
            return self._lookup[structure_family][media_type]
        except KeyError:
            pass
        raise ValueError(
            f"No dispatch for structure_family {structure_family} with media type {media_type}"
        )

    def __call__(self, structure_family, media_type, *args, **kwargs):
        return self.dispatch(structure_family, media_type)(*args, **kwargs)


serialization_registry = Registry()
deserialization_registry = Registry()
