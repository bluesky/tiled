from typing import Any, List, Optional, Tuple, Union, Dict
from datetime import timedelta

import dask.array
from numpy.typing import NDArray

from ..structures.array import ArrayStructure
from ..structures.core import Spec, StructureFamily
from ..structures.table import TableStructure
from ..structures.sparse import COOStructure
from .protocols import AccessPolicy
import pandas
from .type_alliases import JSON, NDSlice
import sparse
from ..server.schemas import SortingItem
from ..structures.core import Spec, StructureFamily
from ..structures.table import TableStructure
from ..structures.awkward import AwkwardStructure
from .awkward_directory_container import DirectoryContainer
from .protocols import AccessPolicy, AnyAdapter
from .type_alliases import JSON
from .utils import IndexersMixin
import collections
MappingType = collections.abc.Mapping


class ArrayAdapter:
    structure_family = StructureFamily.array

    def __init__(
        self,
        array: NDArray[Any],
        structure: ArrayStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        ...

    @classmethod
    def from_array(
        cls,
        array: NDArray[Any],
        *,
        shape: Optional[Tuple[int, ...]] = None,
        chunks: Optional[Tuple[Tuple[int, ...], ...]] = None,
        dims: Optional[Tuple[str, ...]] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "ArrayAdapter":
        ...

class TableAdapter:
    structure_family = StructureFamily.table

    @classmethod
    def from_pandas(
        cls,
        *args: Any,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
        npartitions: int = 1,
        **kwargs: Any,
    ) -> "TableAdapter":
        ...

    @classmethod
    def from_dict(
        cls,
        *args: Any,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
        npartitions: int = 1,
        **kwargs: Any,
    ) -> "TableAdapter":
        ...

    @classmethod
    def from_dask_dataframe(
        cls,
        ddf: dask.dataframe.DataFrame,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "TableAdapter":
        ...

    def __init__(
        self,
        partitions: Union[dask.dataframe.DataFrame, pandas.DataFrame],
        structure: TableStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        ...

class COOAdapter:
    structure_family = StructureFamily.sparse

    @classmethod
    def from_arrays(
        cls,
        coords: NDArray[Any],
        data: Union[dask.dataframe.DataFrame, pandas.DataFrame],
        shape: Tuple[int, ...],
        dims: Optional[Tuple[str, ...]] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "COOAdapter":
        ...

    @classmethod
    def from_coo(
        cls,
        coo: sparse.COO,
        *,
        dims: Optional[Tuple[str, ...]] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "COOAdapter":
        ...

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
        access_policy: Optional[AccessPolicy] = None,
    ) -> "COOAdapter":
        ...

    def __init__(
        self,
        blocks: Dict[Tuple[int, ...], Tuple[NDArray[Any], Any]],
        structure: COOStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        ...

class SparseBlocksParquetAdapter:
    structure_family = StructureFamily.sparse

    def __init__(
        self,
        data_uris: List[str],
        structure: COOStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        ...

class AwkwardAdapter:
    structure_family = StructureFamily.awkward

    def __init__(
        self,
        container: DirectoryContainer,
        structure: AwkwardStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        ...

    @classmethod
    def from_array(
        cls,
        array: NDArray[Any],
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "AwkwardAdapter":
        ...

class AwkwardBuffersAdapter(AwkwardAdapter):
    structure_family = StructureFamily.awkward

    @classmethod
    def from_directory(
        cls,
        data_uri: str,
        structure: AwkwardStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "AwkwardBuffersAdapter":
        ...

class MapAdapter(MappingType[str, AnyAdapter], IndexersMixin):
    structure_family = StructureFamily.container

    def __init__(
        self,
        mapping: Dict[str, Any],
        *,
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        sorting: Optional[List[SortingItem]] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
        entries_stale_after: Optional[timedelta] = None,
        metadata_stale_after: Optional[timedelta] = None,
        must_revalidate: bool = True,
    ) -> None:
        ...

class DatasetAdapter(MapAdapter):

    @classmethod
    def from_dataset(
        cls,
        dataset: Any,
        *,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "DatasetAdapter":
        ...

    def __init__(
        self,
        mapping: Any,
        *args: Any,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
        **kwargs: Any,
    ) -> None:
        ...