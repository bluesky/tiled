import mimetypes
import os
import pathlib
import threading

import watchgod

from .in_memory import Catalog as CatalogInMemory


class TiffReader(ArrayReader):
    def __init__(self, path):
        import tifffile

        super().__init__(tifffile.imread(path))


class Catalog(CatalogInMemory):
    "Make a Catalog from files."

    # TODO Implement some culling of state, possibly with a variant of LazyMap.

    def __init__(self, directory):
        self._watching_thread = None
        super().__init__(*args, **kwargs)
        self.readers_by_mimetype = {"image/tiff": TiffReader}

    def start_watching_thread(self, directory):
        self._watching_thread = threading.Thread(
            target=self._watch, args=(directory,), name="tiled-watch-filesystem-changes"
        )
        self._watching_thread.start()

    def _watch(self, directory):
        for changes in watchgod.watch(directory):
            print(changes)
            # TODO Call _process_file.

    def _process_file(path):
        mimetype, _ = mimetypes.guess_type(filepath)
        reader_class = self.readers_by_mimetype[mimetype]
        reader = reader_class(str(path))
        catalog = self
        for part in path.parent.parts:
            try:
                catalog = catalog[segment]
            except KeyError:
                catalog = catalog._mapping[part] = CatalogInMemory({})
        catalog[path.name] = reader

    @classmethod
    def from_directory(cls, directory):
        """
        Construct a Catalog from a (possibly nested) directory.

        Parameters
        ----------
        directory : Path or str
        """
        for root, dirs, files in os.walk(directory, topdown=False):
            for file in files:
                self._process_file(pathlib.Path(root, file))
