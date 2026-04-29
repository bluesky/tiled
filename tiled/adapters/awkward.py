import copy
from collections.abc import Set
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import awkward
import awkward.forms
from numpy.typing import NDArray

from tiled.adapters.core import Adapter

from ..catalog.orm import Node
from ..storage import DirectoryContainer, FileStorage, Storage
from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource
from ..type_aliases import JSON
from ..utils import path_from_uri
from .utils import init_adapter_from_catalog


class AwkwardAdapter(Adapter[AwkwardStructure]):
    structure_family: StructureFamily = StructureFamily.awkward

    def __init__(
        self,
        data_uri: str,
        structure: AwkwardStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ):
        directory: Path = path_from_uri(data_uri)
        if not directory.is_dir():
            raise ValueError(f"Not a directory: {directory}")
        self._container = DirectoryContainer(directory)
        super().__init__(structure=structure, metadata=metadata, specs=specs)

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[AwkwardStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "AwkwardAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)

    @classmethod
    def from_array(
        cls,
        array: NDArray[Any],
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
        form = awkward.forms.from_dict(self._structure.form)
        keys = [
            key
            for key in form.expected_from_buffers()
            if (form_keys is None)
            or any(key.startswith(form_key) for form_key in form_keys)
        ]

        return {key: self._container[key] for key in keys}

    def read(self) -> Dict[str, bytes]:
        # Return all buffers as a dictionary mapping of form keys to bytes
        return dict(self._container)

    def write(self, data: dict[str, bytes]) -> None:
        # Write each buffer to the corresponding file in the directory
        for form_key, value in data.items():
            self._container[form_key] = value


def is_ragged_form(form: awkward.forms.form.Form) -> bool:
    "Check if an Awkward Form represents a ragged (or a uniform) array structure."
    if isinstance(form, awkward.forms.NumpyForm):
        return True
    elif isinstance(form, awkward.forms.ListOffsetForm):
        return is_ragged_form(form.content)
    else:
        return False
