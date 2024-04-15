import collections.abc
from abc import abstractmethod
from typing import Any, Dict, List, Optional, Protocol, Tuple, Union

import pandas
import sparse
from numpy.typing import NDArray

from ..server.schemas import Principal
from ..structures.array import ArrayStructure
from ..structures.awkward import AwkwardStructure
from ..structures.core import Spec
from ..structures.sparse import SparseStructure
from ..structures.table import TableStructure
from .type_alliases import JSON, Filters, NDSlice, Scopes


class BaseAdapter(Protocol):
    @abstractmethod
    def metadata(self) -> JSON:
        ...

    @abstractmethod
    def specs(self) -> List[Spec]:
        ...


class ContainerAdapter(collections.abc.Mapping[str, "AnyAdapter"], BaseAdapter):
    @abstractmethod
    def structure(self) -> None:
        pass


class ArrayAdapter(BaseAdapter, Protocol):
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
    @abstractmethod
    def structure(self) -> AwkwardStructure:
        pass

    @abstractmethod
    def read(self) -> NDArray:  # Are Slice and Array defined by numpy somewhere?
        pass

    @abstractmethod
    def read_buffers(self, form_keys: Optional[List[str]] = None) -> Dict[str, Any]:
        pass


class SparseAdapter(BaseAdapter, Protocol):
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
    @abstractmethod
    def structure(self) -> TableStructure:
        pass

    @abstractmethod
    def read(self, fields: list[str]) -> pandas.DataFrame:
        pass

    @abstractmethod
    def read_partition(self, partition: int) -> pandas.DataFrame:
        pass

    @abstractmethod
    def get(self, key: str) -> ArrayAdapter:
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
