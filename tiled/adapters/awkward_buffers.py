"""
A directory containing awkward buffers, one file per form key.
"""
from pathlib import Path
from typing import Any, List, Optional

import awkward.forms

from ..catalog.orm import Node
from ..server.schemas import Asset
from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import DataSource
from ..type_aliases import JSON
from ..utils import path_from_uri
from .awkward import AwkwardAdapter
from .awkward_directory_container import DirectoryContainer
from .utils import init_adapter_from_catalog


class AwkwardBuffersAdapter(AwkwardAdapter):
    structure_family = StructureFamily.awkward

    def __init__(
        self,
        data_uri: str,
        structure: AwkwardStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ):
        form = awkward.forms.from_dict(structure.form)
        directory: Path = path_from_uri(data_uri)
        if not directory.is_dir():
            raise ValueError(f"Not a directory: {directory}")
        container = DirectoryContainer(directory, form)
        super().__init__(
            container,
            structure=structure,
            metadata=metadata,
            specs=specs,
        )

    @classmethod
    def init_storage(cls, data_uri: str, structure: AwkwardStructure) -> List[Asset]:
        directory: Path = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        return [Asset(data_uri=data_uri, is_directory=True, parameter="data_uri")]

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource,
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "AwkwardBuffersAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)  # type: ignore
