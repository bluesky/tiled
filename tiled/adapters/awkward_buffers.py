"""
A directory containing awkward buffers, one file per form key.
"""
from pathlib import Path
from typing import Any, List, Optional, Union

import awkward.forms

from ..access_policies import DummyAccessPolicy, SimpleAccessPolicy
from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from ..utils import path_from_uri
from .awkward import AwkwardAdapter
from .awkward_directory_container import DirectoryContainer
from .type_alliases import JSON


class AwkwardBuffersAdapter(AwkwardAdapter):
    """ """

    structure_family = StructureFamily.awkward

    @classmethod
    def init_storage(cls, data_uri: str, structure: AwkwardStructure) -> List[Any]:
        """

        Parameters
        ----------
        data_uri :
        structure :

        Returns
        -------

        """
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
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[Union[DummyAccessPolicy, SimpleAccessPolicy]] = None,
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
