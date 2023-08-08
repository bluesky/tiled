"""
A directory containing awkward buffers, one file per form key.
"""
from urllib import parse

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

    def write(self, data):
        for form_key, value in data.items():
            with open(self.directory / form_key, "wb") as file:
                file.write(value)

    def read(self, form_keys=None):
        selected_suffixed_form_keys = []
        if form_keys is None:
            # Read all.
            selected_suffixed_form_keys.extend(self._structure.suffixed_form_keys)
        else:
            for form_key in form_keys:
                for suffixed_form_key in self._structure.suffixed_form_keys:
                    if suffixed_form_key.startswith(form_key):
                        selected_suffixed_form_keys.append(suffixed_form_key)
        buffers = {}
        for form_key in selected_suffixed_form_keys:
            with open(self.directory / form_key, "rb") as file:
                buffers[form_key] = file.read()
        return buffers

    def structure(self):
        return self._structure
