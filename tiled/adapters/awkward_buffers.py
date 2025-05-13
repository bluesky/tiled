"""
A directory containing awkward buffers, one file per form key.
"""

import copy
from pathlib import Path
from typing import Any, List, Optional
from urllib.parse import quote_plus

import awkward.forms

from ..catalog.orm import Node
from ..storage import FileStorage, Storage
from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource
from ..type_aliases import JSON
from ..utils import path_from_uri
from .awkward import AwkwardAdapter
from .awkward_directory_container import DirectoryContainer
from .utils import init_adapter_from_catalog


class AwkwardBuffersAdapter(AwkwardAdapter):
    structure_family = StructureFamily.awkward
    supported_storage = {FileStorage}

    @classmethod
    def init_storage(
        cls,
        storage: Storage,
        data_source: DataSource[AwkwardStructure],
        path_parts: List[str],
    ) -> DataSource[AwkwardStructure]:
        """

        Parameters
        ----------
        data_uri :
        structure :

        Returns
        -------

        """
        data_source = copy.deepcopy(data_source)  # Do not mutate caller input.
        data_uri = storage.uri + "".join(
            f"/{quote_plus(segment)}" for segment in path_parts
        )
        directory: Path = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        data_source.assets.append(
            Asset(data_uri=data_uri, is_directory=True, parameter="data_uri")
        )
        return data_source

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
    def from_catalog(
        cls,
        data_source: DataSource[AwkwardStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "AwkwardBuffersAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)  # type: ignore
