from .in_memory import Catalog as CatalogInMemory


class Catalog(CatalogInMemory):
    "Make a Catalog from a (nested) directory of files."

    # TODO watchgod

    @classmethod
    def from_directory(cls, directory, globs=None, mime_types=None):
        """
        directory : Path or str
        globs: Dict[str, str]
            Map glob string like ``"*.sqlite"`` to Reader class
        mime_types: Dict[str, str]
            Map MIME type like ``"image/tiff"`` to Reader class
        """
        return cls({})  # TODO
