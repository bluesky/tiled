"""
A directory containing awkward buffers, one file per form key.
"""

import copy
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote_plus

import awkward.forms

from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Storage
from ..type_aliases import JSON
from ..utils import path_from_uri
from .awkward import AwkwardAdapter
from .awkward_directory_container import DirectoryContainer
from .protocols import AccessPolicy


class AwkwardBuffersAdapter(AwkwardAdapter):
    """ """

    structure_family = StructureFamily.awkward

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
        data_uri = str(storage.get("filesystem")) + "".join(
            f"/{quote_plus(segment)}" for segment in path_parts
        )
        directory: Path = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        data_source.assets.append(
            Asset(data_uri=data_uri, is_directory=True, parameter="data_uri")
        )
        return data_source

    @classmethod
    def from_directory(
        cls,
        data_uri: str,
        structure: AwkwardStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "AwkwardBuffersAdapter":
        """

        Parameters
        ----------
        data_uri :
        structure :
        metadata :
        specs :
        access_policy :

        Returns
        -------

        """
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
