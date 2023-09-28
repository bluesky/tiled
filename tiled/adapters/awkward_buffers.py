"""
A directory containing awkward buffers, one file per form key.
"""
import collections.abc
from urllib import parse

import awkward.forms

from ..structures.core import StructureFamily
from .awkward import AwkwardAdapter


class DirectoryContainer(collections.abc.MutableMapping):
    def __init__(self, directory, form):
        self.directory = directory
        self.form = form

    def __getitem__(self, form_key):
        with open(self.directory / form_key, "rb") as file:
            return file.read()

    def __setitem__(self, form_key, value):
        with open(self.directory / form_key, "wb") as file:
            file.write(value)

    def __delitem__(self, form_key):
        (self.directory / form_key).unlink(missing_ok=True)

    def __iter__(self):
        yield from self.form.expected_from_buffers()

    def __len__(self):
        return len(self.form.expected_from_buffers())


class AwkwardBuffersAdapter(AwkwardAdapter):
    structure_family = StructureFamily.awkward

    @classmethod
    def init_storage(cls, directory, structure):
        from ..server.schemas import Asset

        directory.mkdir()
        data_uri = parse.urlunparse(("file", "localhost", str(directory), "", "", None))
        return [Asset(data_uri=data_uri, is_directory=True)]

    @classmethod
    def from_directory(
        cls,
        directory,
        structure,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        form = awkward.forms.from_dict(structure.form)
        container = DirectoryContainer(directory, form)
        return cls(
            container,
            structure=structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )
