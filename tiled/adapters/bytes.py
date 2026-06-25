"""Adapter for the opaque-bytes structure family.

The `bytes` family exists so externally registered files with no useful
logical structure can be cataloged. Bytes nodes are not served as content
through a dedicated endpoint; clients download the underlying assets via
`/asset/bytes/{path}?id=N`, one request per asset, gated by
`settings.expose_raw_assets`. The adapter is therefore minimal: it holds
the declared structure and the catalog wiring, nothing more.
"""

from collections.abc import Set
from typing import Any, Optional

from ..catalog.orm import Node
from ..storage import FileStorage, ObjectStorage, Storage
from ..structures.bytes import BytesStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON
from .core import Adapter


class BytesAdapter(Adapter[BytesStructure]):
    """Adapter for an opaque sequence of bytes spread across one or more assets.

    Read-only by design. The structure carries no fields of its own; the
    per-asset byte length is recorded on each `Asset` (`Asset.size`) and the
    adapter does not expose `read`/`read_stream` -- downloads go through
    `/asset/bytes`.
    """

    structure_family: StructureFamily = StructureFamily.bytes

    def __init__(
        self,
        structure: BytesStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[list[Spec]] = None,
    ) -> None:
        super().__init__(structure=structure, metadata=metadata, specs=specs)

    @classmethod
    def supported_storage(cls) -> Set[type[Storage]]:
        return {FileStorage, ObjectStorage}

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[BytesStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "BytesAdapter":
        return cls(
            structure=data_source.structure,
            metadata=node.metadata_,
            specs=node.specs,
        )
