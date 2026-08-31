from abc import ABC
from collections.abc import Mapping
from typing import Any

from ..utils import filter_known_kwargs


class Structure(ABC):
    @classmethod
    # TODO: When dropping support for Python 3.10 replace with -> Self
    def from_json(cls, structure: Mapping[str, Any]) -> "Structure":
        return cls(**filter_known_kwargs(cls, structure))

    def is_compatible(self, other: "Structure") -> bool:
        """Whether data described by `other` can be served using this structure.

        This is a looser relation than equality: two structures are compatible
        when one may stand in for the other to serve the same underlying data
        (for example, an array reshaped to a different shape, or a table with
        the same columns in a different number of partitions).

        The base implementation accepts any structure of the same type.
        Subclasses (e.g. `ArrayStructure`, `TableStructure`) impose stricter,
        family-specific requirements.
        """
        return isinstance(other, type(self))
