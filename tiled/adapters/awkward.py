import awkward
import awkward.forms

from ..structures.awkward import AwkwardStructure
from ..structures.core import StructureFamily


class AwkwardAdapter:
    structure_family = StructureFamily.awkward

    def __init__(
        self,
        container,
        structure,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        self.container = container
        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])
        self.access_policy = access_policy

    @classmethod
    def from_array(cls, array, metadata=None, specs=None, access_policy=None):
        form, length, container = awkward.to_buffers(array)
        structure = AwkwardStructure(length=length, form=form.to_dict())
        return cls(
            container,
            structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

    def metadata(self):
        return self._metadata

    def read_buffers(self, form_keys=None):
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

    def read(self):
        return dict(self.container)

    def write(self, container):
        for form_key, value in container.items():
            self.container[form_key] = value

    def structure(self):
        return self._structure
