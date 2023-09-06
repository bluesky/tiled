from ..structures.core import StructureFamily


class BytesAdapter:
    """TBD"""

    structure_family = StructureFamily.bytes

    def __init__(
        self,
        buffer: bytes,
        *,
        structure=None,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        self._buffer = buffer
        self._structure = structure
        self._metadata = metadata or {}
        self.specs = specs or []

    @classmethod
    def from_datasource(
        cls,
        datasource,
        *,
        structure=None,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        assets = datasource.assets
        assert assets
        if len(assets) > 1:
            buffer = cls._combine_asset_bytes(assets)
        else:
            buffer = cls._asset_bytes(assets[0])

        return cls(
            buffer,
            structure=structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

    @classmethod
    def from_asset(
        cls,
        asset,
        *,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        # TODO: Do we need this constructor?
        asset_uri = asset.data_uri
        raise NotImplementedError

    def __repr__(self):
        return f"{type(self).__name__}({self._buffer!r})"

    def metadata(self):
        return self._metadata

    def structure(self):
        return self._structure

    def read(self, slice=None):
        # TODO: `slice` parameter is ignored for now

        # TODO: Do we open a file here or offer a classmethod constructor for assets?
        return self._buffer

    def write(self, data):
        raise NotImplementedError
        return self._buffer
