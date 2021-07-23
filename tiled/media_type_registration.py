import collections
import gzip
import mimetypes

from collections import defaultdict

from .utils import DictView, modules_available


class SerializationRegistry:
    """
    Registry of media types for each structure family

    Examples
    --------

    Register a JSON writer for "array" structures.
    (This is included by default but it is shown here as a simple example.)

    >>> import json
    >>>> serialization_registry.register(
        "array", "application/json", lambda array: json.dumps(array.tolist()).encode()
    )

    """

    def __init__(self):
        self._lookup = defaultdict(dict)
        # TODO Think about whether lazy registration makes any sense here.
        self._custom_aliases_by_type = defaultdict(list)
        self._custom_aliases = {}

    def media_types(self, structure_family):
        """
        List the supported media types for a given structure family.
        """
        return DictView(self._lookup[structure_family])

    @property
    def structure_families(self):
        """
        List the known structure families.
        """
        return list(self._lookup)

    def aliases(self, structure_family):
        """
        List the aliases (file extensions) for each media type for a given structure family.
        """
        result = {}
        for media_type in self.media_types(structure_family):
            if media_type == "application/octet-stream":
                # Skip the general binary type; it doesn't really apply.
                continue
            aliases = []
            for k, v in mimetypes.types_map.items():
                # e.g. k, v == (".csv", "text/csv")
                if v == media_type:
                    aliases.append(k[1:])  # e.g. aliases == {"text/csv": ["csv"]}
            if aliases:
                result[media_type] = aliases
        result.update(self._custom_aliases_by_type)
        return result

    def register(self, structure_family, media_type, func):
        """
        Register a new media_type for a structure family.

        Parameters
        ----------
        structure_family : str
            The structure we are encoding, as in "array", "dataframe", "variable", ...
        media_type : str
            MIME type, as in "application/json" or "text/csv".
            If there is not standard name, use "application/x-INVENT-NAME-HERE".
        func : callable
            Should accept the relevant structure as input (e.g. a numpy array)
            and return bytes or memoryview
        """
        self._lookup[structure_family][media_type] = func

    def register_alias(self, ext, media_type):
        self._custom_aliases_by_type[media_type].append(ext)
        self._custom_aliases[ext] = media_type

    def resolve_alias(self, alias):
        try:
            return mimetypes.types_map[f".{alias}"]
        except KeyError:
            return self._custom_aliases.get(alias, alias)

    def dispatch(self, structure_family, media_type):
        """
        Look up a writer for a given structure and media type.
        """
        try:
            return self._lookup[structure_family][media_type]
        except KeyError:
            pass
        raise ValueError(
            f"No dispatch for structure_family {structure_family} with media type {media_type}"
        )

    def __call__(self, structure_family, media_type, *args, **kwargs):
        """
        Invoke a writer for a given structure and media type.
        """
        return self.dispatch(structure_family, media_type)(*args, **kwargs)


class CompressionRegistry:
    def __init__(self):
        self._lookup = defaultdict(collections.OrderedDict)

    def encodings(self, media_type):
        # The last encoding registered is the first preferred by server
        # during content negotiation. Thus, any user-registered
        # ones will get tried first (if the client accepts them).
        return reversed(self._lookup.get(media_type, []))

    def register(self, media_type, encoding, func):
        """
        Register a new media_type for a structure family.

        Parameters
        ----------
        structure_family : str
            The structure we are encoding, as in "array", "dataframe", "variable", ...
        media_type : str
            MIME type, as in "application/json" or "text/csv".
            If there is not standard name, use "application/x-INVENT-NAME-HERE".
        func : callable
            Should accept the relevant structure as input (e.g. a numpy array)
            and return bytes or memoryview
        """
        self._lookup[media_type][encoding] = func

    def dispatch(self, media_type, encoding):
        try:
            return self._lookup[media_type][encoding]
        except KeyError:
            pass
        raise ValueError(
            f"No dispatch for encoding {encoding} for media type {media_type}"
        )

    def __call__(self, media_type, encoder, *args, **kwargs):
        """
        Invoke an encoder.
        """
        return self.dispatch(media_type, encoder)(*args, **kwargs)


serialization_registry = SerializationRegistry()
"Global serialization registry. See Registry for usage examples."

compression_registry = CompressionRegistry()
"Global compression registry. See Registry for usage examples."

# TODO Do we *need* a deserialization registry?
# The Python client always deals with a certain preferred format
# for each structure family. Deserializing other formats is other
# clients' problem, and we can't help with that from here.
deserialization_registry = SerializationRegistry()


compression_registry.register(
    "application/json",
    "gzip",
    lambda buffer: gzip.GzipFile(mode="wb", fileobj=buffer, compresslevel=9),
)
compression_registry.register(
    "application/x-msgpack",
    "gzip",
    lambda buffer: gzip.GzipFile(mode="wb", fileobj=buffer, compresslevel=9),
)
compression_registry.register(
    "application/octet-stream",
    "gzip",
    # Use a lower compression level. High compression is extremely slow
    # (~60 seconds) on large array data.
    lambda buffer: gzip.GzipFile(mode="wb", fileobj=buffer, compresslevel=1),
)
if modules_available("blosc"):

    import blosc

    # The choice of settings here is cribbed from distributed.protocol.compression.
    n = blosc.set_nthreads(2)
    if hasattr("blosc", "releasegil"):
        blosc.set_releasegil(True)
    blosc_settings = {"cname": "lz4", "clevel": 5}

    class BloscBuffer:
        """
        Imitate the API provided by gzip.GzipFile and used by tiled.server.compression.

        It's not clear to me yet what this buys us, but I think we should follow
        the pattern set by starlette until we have a clear reason not to.
        """

        def __init__(self, file):
            self._file = file

        def write(self, b):
            if hasattr(b, "itemsize"):
                # This could be memoryview or numpy.ndarray, for example.
                # Blosc uses item-aware shuffling for improved results.
                compressed = blosc.compress(b, typesize=b.itemsize, **blosc_settings)
            else:
                compressed = blosc.compress(b, **blosc_settings)
            self._file.write(compressed)

        def close(self):
            pass

    compression_registry.register(
        "application/octet-stream",
        "blosc",
        # Use a lower compression level. High compression is extremely slow
        # (~60 seconds) on large array data.
        lambda buffer: BloscBuffer(buffer),
    )
