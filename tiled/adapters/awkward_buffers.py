"""
A directory containing awkward buffers, one file per form key.
"""
import collections.abc

import awkward.forms

from ..structures.core import StructureFamily
from ..utils import path_from_uri
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
    def init_storage(cls, data_uri, structure):
        from ..server.schemas import Asset

        directory = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        return [Asset(data_uri=data_uri, is_directory=True, parameter="data_uri")]

    @classmethod
    def from_directory(
        cls,
        data_uri,
        structure,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        form = awkward.forms.from_dict(structure.form)
        directory = path_from_uri(data_uri)
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
