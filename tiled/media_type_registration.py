from collections import defaultdict

from .utils import DictView


class Registry:
    def __init__(self):
        self._lookup = defaultdict(dict)
        # TODO Think about whether lazy registration makes any sense here.

    def media_types(self, container):
        return DictView(self._lookup[container])

    @property
    def containers(self):
        return list(self._lookup)

    def register(self, container, media_type, func):
        self._lookup[container][media_type] = func

    def dispatch(self, container, media_type):
        try:
            return self._lookup[container][media_type]
        except KeyError:
            pass
        raise ValueError(
            "No dispatch for container {container} with media type {media_type}"
        )

    def __call__(self, container, media_type, *args, **kwargs):
        return self.dispatch(container, media_type)(*args, **kwargs)


serialization_registry = Registry()
deserialization_registry = Registry()
