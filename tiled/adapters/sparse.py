from typing import Any

import dask.dataframe
import numpy
import pandas
import sparse
from numpy._typing import NDArray

from tiled.adapters.core import Adapter

from ..ndslice import NDBlock, NDSlice
from ..structures.array import BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..structures.sparse import COOStructure, SparseStructure
from ..type_aliases import JSON, Chunks


class SparseAdapter(Adapter[SparseStructure]):
    structure_family: StructureFamily = StructureFamily.sparse


class COOAdapter(SparseAdapter):
    "Wrap sparse Coordinate List (COO) arrays."

    @classmethod
    def from_arrays(
        cls,
        coords: NDArray[Any],
        data: dask.dataframe.DataFrame | pandas.DataFrame,
        shape: tuple[int, ...],
        dims: tuple[str, ...] | None = None,
        *,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> "COOAdapter":
        """
        Simplest constructor. Single chunk from coords, data arrays.

        Parameters
        ----------
        coords :
        data :
        shape :
        dims :
        metadata :
        specs :

        Returns
        -------

        """
        structure = COOStructure(
            dims=dims,
            shape=shape,
            chunks=tuple((dim,) for dim in shape),
            data_type=BuiltinDtype.from_numpy_dtype(data.dtype),
            coord_data_type=BuiltinDtype.from_numpy_dtype(coords.dtype),
            resizable=False,
        )
        return cls(
            {(0, 0): (coords, data)},
            structure,
            metadata=metadata,
            specs=specs,
        )

    @classmethod
    def from_coo(
        cls,
        coo: sparse.COO,
        *,
        dims: tuple[str, ...] | None = None,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> "COOAdapter":
        """
        Construct from sparse.COO object.
        Parameters
        ----------
        coo :
        dims :
        metadata :
        specs :

        Returns
        -------

        """
        return cls.from_arrays(
            coords=coo.coords,
            data=coo.data,
            shape=coo.shape,
            dims=dims,
            metadata=metadata,
            specs=specs,
        )

    @classmethod
    def from_global_ref(
        cls,
        blocks: dict[tuple[int, ...], tuple[NDArray[Any], Any]],
        shape: tuple[int, ...],
        chunks: Chunks,
        *,
        dims: tuple[str, ...] | None = None,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> "COOAdapter":
        """
        Construct from blocks with coords given in global reference frame.

        Parameters
        ----------
        blocks :
        shape :
        chunks :
        dims :
        metadata :
        specs :

        Returns
        -------

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
            data_type=BuiltinDtype.from_numpy_dtype(data.dtype),
            coord_data_type=BuiltinDtype.from_numpy_dtype(coords.dtype),
            resizable=False,
        )
        return cls(
            local_blocks,
            structure,
            metadata=metadata,
            specs=specs,
        )

    def __init__(
        self,
        blocks: dict[tuple[int, ...], tuple[NDArray[Any], Any]],
        structure: COOStructure,
        *,
        metadata: JSON | None = None,
        specs: list[Spec] | None = None,
    ) -> None:
        """
        Construct from blocks with coords given in block-local reference frame.

        Parameters
        ----------
        blocks :
        structure :
        metadata :
        specs :
        """
        self.blocks = blocks
        super().__init__(structure, metadata=metadata, specs=specs)

    def read_block(self, block: NDBlock, slice: NDSlice = NDSlice(...)) -> sparse.COO:
        coords, data = self.blocks[block]
        shape = block.shape_from_chunks(self._structure.chunks)
        arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
        return arr[slice] if slice else arr

    def read(self, slice: NDSlice = NDSlice(...)) -> sparse.COO:
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
        return arr[slice] if slice else arr
