from abc import ABC
from collections.abc import Mapping
from typing import Any

from ..utils import filter_known_kwargs


class Structure(ABC):
    @classmethod
    # TODO: When dropping support for Python 3.10 replace with -> Self
    def from_json(cls, structure: Mapping[str, Any]) -> "Structure":
        return cls(**filter_known_kwargs(cls, structure))
