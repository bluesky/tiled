class ValidationRegistry:
    """
    Register validation functions for specs
    """

    def __init__(self) -> None:
        self._lookup = {}

    def register(self, spec: str, func) -> None:
        self._lookup[spec] = func

    def dispatch(self, spec: str):
        try:
            return self._lookup[spec]
        except KeyError:
            pass
        raise ValueError(f"No dispatch for spec {spec}")

    def __call__(self, spec: str):
        return self.dispatch(spec)

    def __contains__(self, spec: str) -> bool:
        return spec in self._lookup


default_validation_registry = ValidationRegistry()
"Global validation registry"


class ValidationError(Exception):
    pass
