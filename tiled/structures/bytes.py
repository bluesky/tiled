from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tiled.structures.root import Structure


@dataclass
class BytesStructure(Structure):
    """Structure describing an opaque sequence of bytes.

    A `bytes` node carries no structural information of its own: the
    payload is opaque and Tiled neither slices nor reshapes it.
    """

    @classmethod
    def from_json(cls, structure: Mapping[str, Any]) -> "BytesStructure":
        return cls()
