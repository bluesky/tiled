import copy
from collections.abc import Set
from pathlib import Path
from typing import Dict, List, Optional, Union
from urllib.parse import quote_plus

import awkward

from ..catalog.orm import Node
from ..storage import DirectoryContainer, FileStorage, Storage
from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource
from ..type_aliases import JSON
from ..utils import path_from_uri
from .core import Adapter


class AwkwardAdapter(Adapter[AwkwardStructure]):
    """In-memory adapter for awkward arrays"""

    structure_family: StructureFamily = StructureFamily.awkward

    def __init__(
        self,
        container: Dict[str, bytes],
        structure: AwkwardStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ):
        self._container = container
        self._array = awkward.from_buffers(
            structure.awk_form, structure.length, container
        )
        super().__init__(structure=structure, metadata=metadata, specs=specs)

    @classmethod
    def from_array(
        cls,
        array: awkward.Array,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> "AwkwardAdapter":
        form, length, container = awkward.to_buffers(array)
        structure = AwkwardStructure(length=length, form=form.to_dict())
        return cls(
            container,
            structure,
            metadata=metadata,
            specs=specs,
        )

    def read_buffers(self, form_keys: Optional[List[str]] = None) -> Dict[str, bytes]:
        keys = [
            key
            for key in self._structure.awk_form.expected_from_buffers()
            if (form_keys is None)
            or any(key.startswith(form_key) for form_key in form_keys)
        ]

        return {key: self._container[key] for key in keys}

    def read(self) -> awkward.Array:
        return self._array


class AwkwardBuffersAdapter(Adapter[AwkwardStructure]):
    structure_family: StructureFamily = StructureFamily.awkward

    def __init__(
        self,
        container: Union[DirectoryContainer, Dict[str, bytes]],
        structure: AwkwardStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ):
        self._container = container
        super().__init__(structure=structure, metadata=metadata, specs=specs)

    @classmethod
    def from_catalog(
        cls, data_source: DataSource[AwkwardStructure], node: Node
    ) -> "AwkwardBuffersAdapter":
        container = DirectoryContainer(path_from_uri(data_source.assets[0].data_uri))
        return cls(
            container, data_source.structure, metadata=node.metadata_, specs=node.specs
        )

    @classmethod
    def supported_storage(cls) -> Set[type[Storage]]:
        return {FileStorage}

    @classmethod
    def init_storage(
        cls,
        storage: Storage,
        data_source: DataSource[AwkwardStructure],
        path_parts: List[str],
    ) -> DataSource[AwkwardStructure]:
        data_source = copy.deepcopy(data_source)  # Do not mutate caller input.
        data_uri = storage.uri + "".join(
            f"/{quote_plus(segment)}" for segment in path_parts
        )
        directory: Path = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        if any(directory.iterdir()):
            raise FileExistsError(f"Directory not empty: {directory}")
        data_source.assets.append(
            Asset(data_uri=data_uri, is_directory=True, parameter="data_uri")
        )
        return data_source

    def read_buffers(self, form_keys: Optional[List[str]] = None) -> Dict[str, bytes]:
        # Return all buffers as a dictionary mapping of form keys to bytes
        keys = [
            key
            for key in self._structure.awk_form.expected_from_buffers()
            if (form_keys is None)
            or any(key.startswith(form_key) for form_key in form_keys)
        ]

        return {key: self._container[key] for key in keys}

    def read(self) -> awkward.Array:
        array = awkward.from_buffers(
            self._structure.awk_form, self._structure.length, self._container
        )
        return array

    def write(self, data: dict[str, bytes]) -> None:
        # Write each buffer to the corresponding file in the directory
        for form_key, value in data.items():
            self._container[form_key] = value
