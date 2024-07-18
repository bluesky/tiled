import collections.abc
import sys
from abc import abstractmethod
from typing import Any, Dict, List, Literal, Optional, Protocol, Tuple, Union

import dask.dataframe
import pandas
import sparse
from numpy.typing import NDArray

from ..server.schemas import Principal
from ..structures.array import ArrayStructure
from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec, StructureFamily
from ..structures.sparse import SparseStructure
from ..structures.table import TableStructure
from .awkward_directory_container import DirectoryContainer
from .type_alliases import JSON, Filters, NDSlice, Scopes


class BaseAdapter(Protocol):
    @abstractmethod
    def metadata(self) -> JSON:
        pass

    @abstractmethod
    def specs(self) -> List[Spec]:
        pass


if sys.version_info < (3, 9):
    from typing_extensions import Mapping

    MappingType = Mapping
else:
    import collections

    MappingType = collections.abc.Mapping


class ContainerAdapter(MappingType[str, "AnyAdapter"], BaseAdapter):
    structure_family: Literal[StructureFamily.container]

    @abstractmethod
    def structure(self) -> None:
        pass


class ArrayAdapter(BaseAdapter, Protocol):
    structure_family: Literal[StructureFamily.array]

    @abstractmethod
    def structure(self) -> ArrayStructure:
        pass

    @abstractmethod
    def read(self, slice: NDSlice) -> NDArray[Any]:
        pass

    # TODO Fix slice
    @abstractmethod
    def read_block(self, block: Tuple[int, ...]) -> NDArray[Any]:
        pass


class AwkwardAdapter(BaseAdapter, Protocol):
    structure_family: Literal[StructureFamily.awkward]

    @abstractmethod
    def structure(self) -> AwkwardStructure:
        pass

    @abstractmethod
    def read(self) -> NDArray[Any]:  # Are Slice and Array defined by numpy somewhere?
        pass

    @abstractmethod
    def read_buffers(self, form_keys: Optional[List[str]] = None) -> Dict[str, Any]:
        pass

    @abstractmethod
    def write(self, container: DirectoryContainer) -> None:
        pass


class SparseAdapter(BaseAdapter, Protocol):
    structure_family: Literal[StructureFamily.sparse] = StructureFamily.sparse

    @abstractmethod
    def structure(self) -> SparseStructure:
        pass

    # TODO Fix slice (just like array)
    def read(
        self, slice: NDSlice
    ) -> sparse.COO:  # Are Slice and Array defined by numpy somewhere?
        pass

    # TODO Fix slice (just like array)
    def read_block(self, block: Tuple[int, ...]) -> sparse.COO:
        pass


class TableAdapter(BaseAdapter, Protocol):
    structure_family: Literal[StructureFamily.table] = StructureFamily.table

    @abstractmethod
    def structure(self) -> TableStructure:
        pass

    @abstractmethod
    def read(
        self, fields: List[str]
    ) -> Union[dask.dataframe.DataFrame, pandas.DataFrame]:
        pass

    @abstractmethod
    def read_partition(
        self,
        partition: int,
        fields: Optional[str] = None,
    ) -> Union[dask.dataframe.DataFrame, pandas.DataFrame]:
        pass

    @abstractmethod
    def __getitem__(self, key: str) -> ArrayAdapter:
        pass


AnyAdapter = Union[
    ArrayAdapter, AwkwardAdapter, ContainerAdapter, SparseAdapter, TableAdapter
]


class AccessPolicy(Protocol):
    @abstractmethod
    def allowed_scopes(self, node: BaseAdapter, principal: Principal) -> Scopes:
        pass

    @abstractmethod
    def filters(
        self, node: BaseAdapter, principal: Principal, scopes: Scopes
    ) -> Filters:
        pass
