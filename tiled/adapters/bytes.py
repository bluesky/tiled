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

    Read-only by design. The main purpose of this adapter is to allow the catalog
    to track and expose externally registered files with no useful logical structure.

    The BytesStructure carries no fields of its own; the
    per-asset byte length is recorded on each `Asset` (`Asset.size`) and the
    adapter does not expose `read`/`read_stream` endpoints; instead, downloads
    go through `/asset/bytes/{path}?id=N`, one request per asset, gated by
    `settings.expose_raw_assets`.
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
