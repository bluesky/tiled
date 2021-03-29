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
