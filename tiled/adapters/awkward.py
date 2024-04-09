from typing import Any, Optional, Union

import awkward
import awkward.forms
from numpy.typing import NDArray

from ..access_policies import DummyAccessPolicy, SimpleAccessPolicy
from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from .awkward_buffers import DirectoryContainer
from .type_alliases import JSON


class AwkwardAdapter:
    structure_family = StructureFamily.awkward

    def __init__(
        self,
        container: DirectoryContainer,
        structure: AwkwardStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[list[Spec]] = None,
        access_policy: Optional[Union[DummyAccessPolicy, SimpleAccessPolicy]] = None,
    ) -> None:
        """

        Parameters
        ----------
        container :
        structure :
        metadata :
        specs :
        access_policy :
        """
        self.container = container
        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])
        self.access_policy = access_policy

    @classmethod
    def from_array(
        cls,
        array: NDArray[Any],
        metadata: Optional[JSON] = None,
        specs: Optional[list[Spec]] = None,
        access_policy: Optional[Union[DummyAccessPolicy, SimpleAccessPolicy]] = None,
    ) -> "AwkwardAdapter":
        """

        Parameters
        ----------
        array :
        metadata :
        specs :
        access_policy :

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
            access_policy=access_policy,
        )

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        return self._metadata

    def read_buffers(self, form_keys: Optional[list[str]] = None) -> dict[str, Any]:
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

    def read(self) -> dict[str, Any]:
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
        """

        Returns
        -------

        """
        return self._structure
