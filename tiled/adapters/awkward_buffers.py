"""
A directory containing awkward buffers, one file per form key.
"""
from urllib import parse

import awkward.forms

from ..structures.core import StructureFamily


class AwkwardBuffersAdapter:
    structure_family = StructureFamily.awkward

    def __init__(
        self,
        directory,
        structure,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        self.directory = directory
        self._metadata = metadata or {}
        self._structure = structure
        self.specs = list(specs or [])
        self.access_policy = access_policy

    def metadata(self):
        return self._metadata

    @classmethod
    def init_storage(cls, directory, structure):
        from ..server.schemas import Asset

        directory.mkdir()
        data_uri = parse.urlunparse(("file", "localhost", str(directory), "", "", None))
        return [Asset(data_uri=data_uri, is_directory=True)]

    def write(self, container):
        for form_key, value in container.items():
            with open(self.directory / form_key, "wb") as file:
                file.write(value)

    def read_buffers(self, form_keys=None):
        form = awkward.forms.from_dict(self._structure.form)
        keys = [
            key
            for key in form.expected_from_buffers()
            if (form_keys is None)
            or any(key.startswith(form_key) for form_key in form_keys)
        ]
        container = {}
        for key in keys:
            with open(self.directory / key, "rb") as file:
                container[key] = file.read()
        return container

    def read(self):
        form = awkward.forms.from_dict(self._structure.form)
        container = {}
        for key in form.expected_from_buffers():
            with open(self.directory / key, "rb") as file:
                container[key] = file.read()
        return container

    def structure(self):
        return self._structure
