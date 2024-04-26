import itertools
from typing import Any, List, Optional, Tuple, Union

import dask.base
import dask.dataframe
import numpy
import pandas
import sparse
from numpy._typing import NDArray

from ..adapters.array import slice_and_shape_from_block_and_chunks
from ..server.schemas import Asset
from ..structures.core import Spec, StructureFamily
from ..structures.sparse import COOStructure
from ..utils import path_from_uri
from .protocols import AccessPolicy
from .type_alliases import JSON, NDSlice


def load_block(uri: str) -> Tuple[List[int], Tuple[NDArray[Any], Any]]:
    """

    Parameters
    ----------
    uri :

    Returns
    -------

    """
    # TODO This can be done without pandas.
    # Better to use a plain I/O library.
    df = pandas.read_parquet(path_from_uri(uri))
    coords = df[df.columns[:-1]].values.T
    data = df["data"].values
    return coords, data


class SparseBlocksParquetAdapter:
    """ """

    structure_family = StructureFamily.sparse

    def __init__(
        self,
        data_uris: List[str],
        structure: COOStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """

        Parameters
        ----------
        data_uris :
        structure :
        metadata :
        specs :
        access_policy :
        """
        num_blocks = (range(len(n)) for n in structure.chunks)
        self.blocks = {}
        for block, uri in zip(itertools.product(*num_blocks), data_uris):
            self.blocks[block] = uri
        self._structure = structure
        self._metadata = metadata or {}
        self.specs = list(specs or [])
        self.access_policy = access_policy

    @classmethod
    def init_storage(
        cls,
        data_uri: str,
        structure: COOStructure,
    ) -> List[Asset]:
        """

        Parameters
        ----------
        data_uri :
        structure :

        Returns
        -------

        """
        directory = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)

        num_blocks = (range(len(n)) for n in structure.chunks)
        assets = [
            Asset(
                data_uri=f"{data_uri}/block-{'.'.join(map(str, block))}.parquet",
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
            for i, block in enumerate(itertools.product(*num_blocks))
        ]
        return assets

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        return self._metadata

    def write_block(
        self,
        data: Union[dask.dataframe.DataFrame, pandas.DataFrame],
        block: Tuple[int, ...],
    ) -> None:
        """

        Parameters
        ----------
        data :
        block :

        Returns
        -------

        """
        uri = self.blocks[block]
        data.to_parquet(path_from_uri(uri))

    def write(self, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]) -> None:
        """

        Parameters
        ----------
        data :

        Returns
        -------

        """
        if len(self.blocks) > 1:
            raise NotImplementedError
        uri = self.blocks[(0,) * len(self._structure.shape)]
        data.to_parquet(path_from_uri(uri))

    def read(self, slice: NDSlice) -> sparse.COO:
        """

        Parameters
        ----------
        slice :

        Returns
        -------

        """
        all_coords = []
        all_data = []
        for block, uri in self.blocks.items():
            coords, data = load_block(uri)
            offsets = []
            for b, c in zip(block, self.structure().chunks):
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
        return arr[slice]

    def read_block(
        self, block: Tuple[int, ...], slice: Optional[NDSlice]
    ) -> sparse.COO:
        """

        Parameters
        ----------
        block :
        slice :

        Returns
        -------

        """
        coords, data = load_block(self.blocks[block])
        _, shape = slice_and_shape_from_block_and_chunks(block, self._structure.chunks)
        arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
        return arr[slice]

    def structure(self) -> COOStructure:
        """

        Returns
        -------

        """
        return self._structure
