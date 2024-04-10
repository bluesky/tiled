import collections.abc
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, TypedDict, Union
from protcols import Protocol, abstractproperty

from numpy.typing import NDArray
import pandas
import sparse

from ..server.schemas import Principal
from ..structures.core import StructureFamily
from ..structures.array import ArrayStructure
from ..structures.awkward import AwkwardStructure
from ..structures.sparse import SparseStructure
from ..structures.table import TableStructure
from .type_aliases import JSON


Spec = TypedDict({"name": str, "version": str})
Slice = Any  # TODO Replace this with our Union for a slice/tuple/.../etc.


class BaseAdapter(Protocol):
    structure_family: StructureFamily

    def metadata(self) -> JSON:
        pass

    @abstractproperty
    def specs(self) -> List[Spec]:
        pass


class ContainerAdapter(collections.abc.Mapping[str, "AnyAdapter"], BaseAdapter):
    structure_family = Literal[StructureFamily.container]

    def structure(self) -> None:
        pass


class ArrayAdapter(BaseAdapter):
    structure_family = Literal[StructureFamily.array]

    def structure(self) -> ArrayStructure:
        pass

    # TODO Fix slice
    def read(self, slice: Slice) -> NDArray:
        pass

    # TODO Fix slice
    def read_block(self, block: Tuple[int, ...]) -> NDArray:
        pass


class AwkwardAdapter(BaseAdapter):
    structure_family = Literal[StructureFamily.awkward]

    def structure(self) -> AwkwardStructure:
        pass

    def read(self) -> NDArray:  # Are Slice and Array defined by numpy somewhere?
        pass

    def read_buffers(self, form_keys: Optional[List[str]] = None) -> Dict[str, Any]:
        pass


class SparseAdapter(BaseAdapter):
    structure_family = Literal[StructureFamily.sparse]

    def structure(self) -> SparseStructure:
        pass

    # TODO Fix slice (just like array)
    def read(
        self, slice: Slice
    ) -> sparse.COO:  # Are Slice and Array defined by numpy somewhere?
        pass

    # TODO Fix slice (just like array)
    def read_block(self, block: Tuple[int, ...]) -> sparse.COO:
        pass


class TableAdapter(BaseAdapter):
    structure_family = Literal[StructureFamily.table]

    def structure(self) -> TableStructure:
        pass

    def read(self, fields: list[str]) -> pandas.DataFrame:
        pass

    def read_partition(self, partition: int) -> pandas.DataFrame:
        pass

    def get(self, key: str) -> ArrayAdapter:
        pass


AnyAdapter = Union[
    ArrayAdapter, AwkwardAdapter, ContainerAdapter, SparseAdapter, TableAdapter
]


Scopes = Set[str]
Query = Any  # for now...
Filters = List[Query]


class AccessPolicy(Protocol):
    def allowed_scopes(self, node: BaseAdapter, principal: Principal) -> Scopes:
        pass

    def filters(
        self, node: BaseAdapter, principal: Principal, scopes: Scopes
    ) -> Filters:
        pass
