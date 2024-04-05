import itertools
from typing import Any, Optional, Tuple, Union

import dask.base
import dask.dataframe
import numpy
import pandas
import sparse
from numpy._typing import NDArray

from ..access_policies import DummyAccessPolicy, SimpleAccessPolicy
from ..adapters.array import slice_and_shape_from_block_and_chunks
from ..structures.core import StructureFamily
from ..structures.sparse import COOStructure
from ..utils import path_from_uri
from .type_alliases import JSON, Spec


def load_block(uri: str) -> Tuple[list[int], Tuple[NDArray[Any], Any]]:
    # TODO This can be done without pandas.
    # Better to use a plain I/O library.
    df = pandas.read_parquet(path_from_uri(uri))
    coords = df[df.columns[:-1]].values.T
    data = df["data"].values
    return coords, data


class SparseBlocksParquetAdapter:
    structure_family = StructureFamily.sparse

    def __init__(
        self,
        data_uris: Union[str, list[str]],
        structure: COOStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[list[Spec]] = None,
        access_policy: Optional[Union[SimpleAccessPolicy, DummyAccessPolicy]] = None,
    ) -> None:
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
        data_uri: Union[str, list[str]],
        structure: COOStructure,
    ) -> Any:
        from ..server.schemas import Asset

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
        return self._metadata

    def write_block(
        self,
        data: Union[dask.dataframe.DataFrame, pandas.DataFrame],
        block: Tuple[int, ...],
    ) -> None:
        uri = self.blocks[block]
        data.to_parquet(path_from_uri(uri))

    def write(self, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]) -> None:
        if len(self.blocks) > 1:
            raise NotImplementedError
        uri = self.blocks[(0,) * len(self._structure.shape)]
        data.to_parquet(path_from_uri(uri))

    def read(self, slice: Optional[Union[int, slice]]) -> NDArray[Any]:
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
        self, block: Tuple[int, ...], slice: Optional[Union[int, slice]]
    ) -> NDArray[Any]:
        coords, data = load_block(self.blocks[block])
        _, shape = slice_and_shape_from_block_and_chunks(block, self._structure.chunks)
        arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
        return arr[slice]

    def structure(self) -> COOStructure:
        return self._structure
