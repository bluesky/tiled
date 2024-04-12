"""
A directory containing awkward buffers, one file per form key.
"""
from pathlib import Path
from typing import List, Optional

import awkward.forms

from ..server.schemas import Asset
from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from ..utils import path_from_uri
from .awkward import AwkwardAdapter
from .awkward_directory_container import DirectoryContainer
from .protocols import AccessPolicy
from .type_alliases import JSON


class AwkwardBuffersAdapter(AwkwardAdapter):
    """ """

    structure_family = StructureFamily.awkward

    @classmethod
    def init_storage(cls, data_uri: str, structure: AwkwardStructure) -> List[Asset]:
        """

        Parameters
        ----------
        data_uri :
        structure :

        Returns
        -------

        """
        directory: Path = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        return [Asset(data_uri=data_uri, is_directory=True, parameter="data_uri")]

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
