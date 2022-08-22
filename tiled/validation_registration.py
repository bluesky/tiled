class ValidationRegistry:
    """
    Register validation functions for specs
    """

    def __init__(self):
        self._lookup = {}

    def register(self, spec, func):
        self._lookup[spec] = func

    def dispatch(self, spec):
        try:
            return self._lookup[spec]
        except KeyError:
            pass
        raise ValueError(f"No dispatch for spec {spec}")

    def __call__(self, spec):
        return self.dispatch(spec)

    def __contains__(self, spec):
        return spec in self._lookup


validation_registry = ValidationRegistry()
"Global validation registry"


class ValidationError(Exception):
    pass
