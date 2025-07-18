from typing import Any, Dict, List, Optional, Set, Tuple, Union

import dask.dataframe
import numpy
import pandas
import sparse
from numpy._typing import NDArray

from ..ndslice import NDSlice
from ..storage import Storage
from ..structures.array import BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..structures.sparse import COOStructure
from ..type_aliases import JSON
from .array import slice_and_shape_from_block_and_chunks


class COOAdapter:
    "Wrap sparse Coordinate List (COO) arrays."
    structure_family = StructureFamily.sparse
    supported_storage: Set[type[Storage]] = set()

    @classmethod
    def from_arrays(
        cls,
        coords: NDArray[Any],
        data: Union[dask.dataframe.DataFrame, pandas.DataFrame],
        shape: Tuple[int, ...],
        dims: Optional[Tuple[str, ...]] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
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
        dims: Optional[Tuple[str, ...]] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
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
        blocks: Dict[Tuple[int, ...], Tuple[NDArray[Any], Any]],
        shape: Tuple[int, ...],
        chunks: Tuple[Tuple[int, ...], ...],
        *,
        dims: Optional[Tuple[str, ...]] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
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
        blocks: Dict[Tuple[int, ...], Tuple[NDArray[Any], Any]],
        structure: COOStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
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
        self._metadata = metadata or {}
        self._structure = structure
        self.specs = specs or []

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        return self._metadata

    def structure(self) -> COOStructure:
        """

        Returns
        -------

        """
        return self._structure

    def read_block(
        self, block: Tuple[int, ...], slice: NDSlice = NDSlice(...)
    ) -> sparse.COO:
        """

        Parameters
        ----------
        block :
        slice :

        Returns
        -------

        """
        coords, data = self.blocks[block]
        _, shape = slice_and_shape_from_block_and_chunks(block, self._structure.chunks)
        arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
        return arr[slice] if slice else arr

    def read(self, slice: NDSlice = NDSlice(...)) -> sparse.COO:
        """

        Parameters
        ----------
        slice :

        Returns
        -------

        """
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
