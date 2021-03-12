from .in_memory import Catalog as CatalogInMemory


class Catalog(CatalogInMemory):
    "Make a Catalog from files."

    # TODO Use watchgod to observe updates.

    @classmethod
    def from_directory(
        cls, directory, reader_for_glob=None, reader_for_mimetype=None, sniffer=None
    ):
        """
        Construct a Catalog from a (possibly nested) directory.

        Parameters
        ----------
        directory : Path or str
        reader_for_glob: Dict[str, str]
            Map glob string like ``"*.sqlite"`` to Reader class
        reader_for_mimetype: Dict[str, str]
            Map MIME type like ``"image/tiff"`` to Reader class
        sniffer: callable
            Expected signature::
                f(path) -> str  # e.g "text/plain"
        """
        return cls({})  # TODO

    @classmethod
    def from_files(
        cls, *globs, reader_for_glob=None, reader_for_mimetype=None, sniffer=None
    ):
        """
        Construct a Catalog from file paths or globs. See examples.

        Parameters
        ----------
        *globs
        reader_for_glob: Dict[str, str]
            Map glob string like ``"*.sqlite"`` to Reader class
        reader_for_mimetype: Dict[str, str]
            Map MIME type like ``"image/tiff"`` to Reader class
        sniffer: callable
            Expected signature::
                f(path) -> str  # e.g "text/plain"

        >>> Catalog.from_globs("things.txt", "stuff.txt", "miscellany.csv")

        >>> Catalog.from_globs("*.txt", "*.csv")
        """
        import glob

        if sniffer is None:
            sniffer = default_sniffer
        if reader_for_glob is None:
            reader_for_glob = {}
        if reader_for_mimetype is None:
            reader_for_mimetype = {}

        mapping = {}
        filepaths = []
        for g in globs:
            filepaths.extend(glob.glob(g))
        for filepath in filepaths:
            mimetype = default_sniffer(filepath)
            if mimetype is None:
                continue
            reader_class = reader_for_glob.get(mimetype)
            if reader_class is not None:
                mapping[filepath] = reader_class(filepath)
                continue
            reader_class = reader_for_mimetype.get(mimetype)
            if reader_class is not None:
                mapping[filepath] = reader_class(filepath)
        return cls(mapping)


def default_sniffer(path):
    import mimetypes

    mimetype, _ = mimetypes.guess_type(path)
    return mimetype
