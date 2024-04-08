"""
A directory containing awkward buffers, one file per form key.
"""
import collections.abc
from pathlib import Path
from typing import Any, Iterator, List, Optional, Union

import awkward.forms
from type_alliases import JSON

from ..access_policies import DummyAccessPolicy, SimpleAccessPolicy
from ..server.pydantic_awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from ..utils import path_from_uri
from .awkward import AwkwardAdapter


class DirectoryContainer(collections.abc.MutableMapping[str, JSON]):
    def __init__(self, directory: Path, form: Any):
        self.directory = directory
        self.form = form

    def __getitem__(self, form_key: str) -> JSON:
        with open(self.directory / form_key, "rb") as file:
            return file.read()

    def __setitem__(self, form_key: str, value: JSON) -> None:
        with open(self.directory / form_key, "wb") as file:
            file.write(value)

    def __delitem__(self, form_key: str) -> None:
        (self.directory / form_key).unlink(missing_ok=True)

    def __iter__(self) -> Iterator[str]:
        yield from self.form.expected_from_buffers()

    def __len__(self) -> int:
        return len(self.form.expected_from_buffers())


class AwkwardBuffersAdapter(AwkwardAdapter):
    structure_family = StructureFamily.awkward

    @classmethod
    def init_storage(cls, data_uri: str, structure: AwkwardStructure) -> List[Any]:
        from ..server.schemas import Asset

        directory: Path = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        return [Asset(data_uri=data_uri, is_directory=True, parameter="data_uri")]

    @classmethod
    def from_directory(
        cls,
        data_uri: str,
        structure: AwkwardStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[list[Spec]] = None,
        access_policy: Optional[Union[DummyAccessPolicy, SimpleAccessPolicy]] = None,
    ) -> "AwkwardBuffersAdapter":
        form = awkward.forms.from_dict(structure.form)
        directory: Path = path_from_uri(data_uri)
        if not directory.is_dir():
            raise ValueError(f"Not a directory: {directory}")
        container = DirectoryContainer(directory, form)
        return cls(
            container,
            structure=structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )
