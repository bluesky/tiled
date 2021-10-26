from .client import from_uri
from intake.catalog import Catalog
from intake.source import DataSource


class TiledCatalog(Catalog):

    def __init__(self, uri: str, path=None):
        if uri.startswith("tiled"):
            uri = uri.replace("tiled", "http", 1)
        self.uri = uri
        self.path = path
        client = from_uri(uri, "dask")
        if path is not None:
            client = client[path]
        super().__init__(entries=client, name="tiled:" + uri.split(":", 1)[1])

    def search(self, query, type="text"):
        if type == "text":
            from tiled.queries import FullText
            q = FullText(query)
        else:
            raise NotImplementedError
        return TiledCatalog.from_dict(self._entries.search(q), uri=self.uri, path=self.path)

    def __getitem__(self, item):
        node = self._entries[item]
        return TiledSource(uri=self.uri, path=item, instance=node)


types = {
    "DaskArrayClient": "ndarray",
    "DaskDatasetClient": "xarray",
    "DaskVariableClient": "xarray",
    "DaskDataFrameClient": "dataframe"
}


class TiledSource(DataSource):

    def __init__(self, uri, path, instance=None, metadata=None):
        if instance is None:
            instance = from_uri(uri, "dask")[path].read()
        self.instance = instance
        md = dict(instance.metadata)
        if metadata:
            md.update(metadata)
        super().__init__(metadata=md)
        self.name = path
        self.container = types[type(self.instance).__name__]

    def discover(self):
        dt = getattr(self.to_dask(), "dtype", None) or getattr(self.to_dask(), "dtypes", None)
        return dict(dtype=dt,
                    shape=getattr(self.instance.structure().macro, "shape", None),
                    npartitions=self.to_dask().npartitions,
                    metadata=self.metadata)

    def to_dask(self):
        return self.instance.read()

    def read(self):
        return self.instance.read().compute()

    def _yaml(self):
        y = super()._yaml()
        v = list(y['sources'].values())[0]
        v['args'].pop('instance')
        return y
