from collections import Counter
from typing import Callable

from .structures.core import Spec, StructureFamily


class ValidationRegistry:
    """
    Register validation functions for specs
    """

    def __init__(self):
        self._lookup = {}

    def register(self, spec: Spec, func: Callable):
        self._lookup[spec] = func

    def dispatch(self, spec: Spec):
        try:
            return self._lookup[spec]
        except KeyError:
            pass
        raise ValueError(f"No dispatch for spec {spec}")

    def __call__(self, spec: Spec):
        return self.dispatch(spec)

    def __contains__(self, spec: Spec):
        return spec in self._lookup


class ValidationError(Exception):
    pass


async def validate_composite(spec, metadata, entry, structure_family, structure):
    """Spec validator for Composite containers

    Imposes rules on the contents of the entry (parent container):

    1. The spec can be assigned only to containers.
    2. No nested containers are allowed.
    3. The column names of all table structures must be unique and not
       conflict with the names of any other contents.
    """

    if entry is None:
        # Creating a new node (container)
        if structure_family != StructureFamily.container:
            raise ValidationError(
                f"Composite spec can be assigned only to containers, not to"
                f" {structure_family}."
            )
    else:
        # Updating an existing node (container)
        if entry.structure_family != StructureFamily.container:
            raise ValidationError(
                f"Composite spec can be assigned only to containers, not to"
                f" {entry.structure_family}."
            )

        flat_name_space = []
        for key, item in await entry.items_range(offset=0, limit=None):
            flat_name_space.append(key)

            if item.structure_family == StructureFamily.table:
                flat_name_space.extend(item.structure().columns)

            elif item.structure_family == StructureFamily.container:
                raise ValidationError(
                    "Nested containers are not allowed in a composite container."
                )

        counts = Counter(flat_name_space)
        if repeats := [item for item, count in counts.items() if count > 1]:
            raise ValidationError(
                "Names of table columns and other items in a composite container"
                " must be unique and not conflict with each other. "
                f"Found conflicting names: {repeats}."
            )


# Global instance of the validation registry
default_validation_registry = ValidationRegistry()
default_validation_registry.register(Spec("composite", None), validate_composite)
