import numpy
import sparse

from ..structures.core import StructureFamily
from ..structures.sparse import COOStructure
from .array import slice_and_shape_from_block_and_chunks


class COOAdapter:
    "Wrap sparse Coordinate List (COO) arrays."
    structure_family = StructureFamily.sparse

    @classmethod
    def from_arrays(
        cls,
        coords,
        data,
        shape,
        dims=None,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        """
        Simplest constructor. Single chunk from coords, data arrays.
        """
        structure = COOStructure(
            dims=dims,
            shape=shape,
            chunks=tuple((dim,) for dim in shape),
            resizable=False,
        )
        return cls(
            {(0, 0): (coords, data)},
            structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

    @classmethod
    def from_coo(cls, coo, *, dims=None, metadata=None, specs=None, access_policy=None):
        "Construct from sparse.COO object."
        return cls.from_arrays(
            coords=coo.coords,
            data=coo.data,
            shape=coo.shape,
            dims=dims,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

    @classmethod
    def from_global_ref(
        cls,
        blocks,
        shape,
        chunks,
        *,
        dims=None,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        """
        Construct from blocks with coords given in global reference frame.
        """
        local_blocks = {}
        for block, (coords, data) in blocks.items():
            offsets = []
            for b, c in zip(block, chunks):
                offset = sum(c[:b])
                offsets.append(offset)
            local_coords = coords - [[i] for i in offsets]
            local_blocks[block] = local_coords, data
        structure = COOStructure(
            dims=dims,
            shape=shape,
            chunks=chunks,
            resizable=False,
        )
        return cls(
            local_blocks,
            structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
        )

    def __init__(
        self,
        blocks,
        structure,
        *,
        metadata=None,
        specs=None,
        access_policy=None,
    ):
        """
        Construct from blocks with coords given in block-local reference frame.
        """
        self.blocks = blocks
        self._metadata = metadata or {}
        self._structure = structure
        self.specs = specs or []
        self.access_policy = access_policy

    def metadata(self):
        return self._metadata

    def structure(self):
        return self._structure

    def read_block(self, block, slice=None):
        coords, data = self.blocks[block]
        _, shape = slice_and_shape_from_block_and_chunks(block, self._structure.chunks)
        arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
        if slice:
            arr = arr[slice]
        return arr

    def read(self, slice=None):
        all_coords = []
        all_data = []
        for block, (coords, data) in self.blocks.items():
            offsets = []
            for b, c in zip(block, self._structure.chunks):
                offset = sum(c[:b])
                offsets.append(offset)
            global_coords = coords + [[i] for i in offsets]
            all_coords.append(global_coords)
            all_data.append(data)
        arr = sparse.COO(
            data=numpy.concatenate(all_data),
            coords=numpy.concatenate(all_coords, axis=-1),
            shape=self._structure.shape,
        )
        if slice:
            return arr[slice]
        return arr
