import sparse

from ..structures.array import ArrayStructure
from ..structures.sparse import COOStructure
from .array import ArrayAdapter


class COOAdapter:
    structure_family = "sparse"

    @classmethod
    def from_coo(cls, coo):
        "Construct from sparse.COO object."
        return cls(coo.data, coo.coords, shape=coo.shape)

    def __init__(self, data, coords, shape, *, dims=None, metadata=None, specs=None):
        self.data = ArrayAdapter.from_array(data)
        self.coords = ArrayAdapter.from_array(coords)
        self.shape = shape
        self.dims = dims
        self.metadata = metadata or {}
        self.specs = specs or []

    def __getitem__(self, key):
        if key == "data":
            return self.data
        if key == "coords":
            return self.coords
        else:
            raise KeyError(key)

    def structure(self):
        return COOStructure(
            coords=ArrayStructure(
                macro=self.coords.macrostructure(), micro=self.coords.microstructure()
            ),
            data=ArrayStructure(
                macro=self.data.macrostructure(), micro=self.data.microstructure()
            ),
            dims=self.dims,
            shape=self.shape,
            chunks=tuple(
                (dim,) for dim in self.shape
            ),  # hard-code to one chunk for one
            resizable=False,
        )

    def read(self, slice=None):
        arr = sparse.COO(
            data=self.data.read(), coords=self.coords.read(), shape=self.shape
        )
        if slice:
            return arr[slice]
        return arr
