from typing import Any, Dict, List, Optional

import awkward
import awkward.forms
from numpy.typing import NDArray

from ..storage import FileStorage
from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from ..type_aliases import JSON
from .awkward_directory_container import DirectoryContainer


class AwkwardAdapter:
    structure_family = StructureFamily.awkward
    supported_storage = {FileStorage}

    def __init__(
        self,
        container: DirectoryContainer,
        structure: AwkwardStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        """

        Parameters
        ----------
        container :
        structure :
        metadata :
        specs :
        """
        self.container = container
        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])

    @classmethod
    def from_array(
        cls,
        array: NDArray[Any],
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> "AwkwardAdapter":
        """

        Parameters
        ----------
        array :
        metadata :
        specs :

        Returns
        -------

        """
        form, length, container = awkward.to_buffers(array)
        structure = AwkwardStructure(length=length, form=form.to_dict())
        return cls(
            container,
            structure,
            metadata=metadata,
            specs=specs,
        )

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        return self._metadata

    def read_buffers(self, form_keys: Optional[List[str]] = None) -> Dict[str, bytes]:
        """

        Parameters
        ----------
        form_keys :

        Returns
        -------

        """
        form = awkward.forms.from_dict(self._structure.form)
        keys = [
            key
            for key in form.expected_from_buffers()
            if (form_keys is None)
            or any(key.startswith(form_key) for form_key in form_keys)
        ]
        buffers = {}
        for key in keys:
            buffers[key] = self.container[key]
        return buffers

    def read(self) -> Dict[str, bytes]:
        """

        Returns
        -------

        """
        return dict(self.container)

    def write(self, container: DirectoryContainer) -> None:
        """

        Parameters
        ----------
        container :

        Returns
        -------

        """
        for form_key, value in container.items():
            self.container[form_key] = value

    def structure(self) -> AwkwardStructure:
        return self._structure
