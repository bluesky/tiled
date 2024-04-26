from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dask.dataframe
import numpy
import pandas
import sparse
from numpy.typing import NDArray
from pytest_mock import MockFixture

from tiled.access_policies import ALL_ACCESS, ALL_SCOPES
from tiled.adapters.awkward_directory_container import DirectoryContainer
from tiled.adapters.protocols import (
    AccessPolicy,
    ArrayAdapter,
    AwkwardAdapter,
    BaseAdapter,
    SparseAdapter,
    TableAdapter,
)
from tiled.adapters.type_alliases import JSON, Filters, NDSlice, Scopes
from tiled.server.schemas import Principal, PrincipalType
from tiled.structures.array import ArrayStructure, BuiltinDtype
from tiled.structures.awkward import AwkwardStructure
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.sparse import COOStructure
from tiled.structures.table import TableStructure


class CustomArrayAdapter:
    structure_family: Literal[StructureFamily.array] = StructureFamily.array

    def __init__(
        self,
        array: NDArray[Any],
        structure: ArrayStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        self._array = array
        self._structure = structure
        self._metadata = metadata or {}
        self._specs = specs or []

    def structure(self) -> ArrayStructure:
        return self._structure

    def read(self, slice: NDSlice) -> NDArray[Any]:
        return self._array

    def read_block(self, block: Tuple[int, ...]) -> NDArray[Any]:
        return self._array

    def specs(self) -> List[Spec]:
        return self._specs

    def metadata(self) -> JSON:
        return self._metadata


def arrayadapter_protocol_functions(
    adapter: ArrayAdapter, slice: NDSlice, block: Tuple[int, ...]
) -> None:
    adapter.structure()
    adapter.read(slice)
    adapter.read_block(block)
    adapter.specs()
    adapter.metadata()


def test_arrayadapter_protocol(mocker: MockFixture) -> None:
    mock_call = mocker.patch.object(CustomArrayAdapter, "structure")
    mock_call2 = mocker.patch.object(CustomArrayAdapter, "read")
    mock_call3 = mocker.patch.object(CustomArrayAdapter, "read_block")
    mock_call4 = mocker.patch.object(CustomArrayAdapter, "specs")
    mock_call5 = mocker.patch.object(CustomArrayAdapter, "metadata")

    structure = ArrayStructure(
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("int32")),
        shape=(2, 512, 512),
        chunks=((1, 1), (512,), (512,)),
        dims=("time", "x", "y"),  # optional
    )

    array = numpy.random.rand(2, 512, 512)
    metadata: JSON = {"foo": "bar"}
    anyslice = (1, 1, 1)
    anyblock = (1, 1, 1)

    anyarrayadapter = CustomArrayAdapter(array, structure, metadata=metadata)
    assert anyarrayadapter.structure_family == StructureFamily.array

    arrayadapter_protocol_functions(anyarrayadapter, anyslice, anyblock)
    mock_call.assert_called_once()
    mock_call2.assert_called_once_with(anyslice)
    mock_call3.assert_called_once_with(anyblock)
    mock_call4.assert_called_once()
    mock_call5.assert_called_once()


class CustomAwkwardAdapter:
    structure_family: Literal[StructureFamily.awkward] = StructureFamily.awkward

    def __init__(
        self,
        container: DirectoryContainer,
        structure: AwkwardStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        self.container = container
        self._metadata = metadata or {}
        self._structure = structure
        self._specs = list(specs or [])
        self.access_policy = access_policy

    def structure(self) -> AwkwardStructure:
        return self._structure

    def read(self) -> NDArray[Any]:
        return numpy.random.rand(4)

    def read_buffers(self, form_keys: Optional[List[str]] = None) -> Dict[str, Any]:
        return {"a": 123}

    def write(self, container: DirectoryContainer) -> None:
        return None

    def specs(self) -> List[Spec]:
        return self._specs

    def metadata(self) -> JSON:
        return self._metadata


def awkwardadapter_protocol_functions(
    adapter: AwkwardAdapter,
    slice: NDSlice,
    form_keys: Optional[List[str]],
    container: DirectoryContainer,
) -> None:
    adapter.structure()
    adapter.read()
    adapter.read_buffers(form_keys)
    adapter.write(container)
    adapter.specs()
    adapter.metadata()


def test_awkwardadapter_protocol(mocker: MockFixture) -> None:
    mock_call = mocker.patch.object(CustomAwkwardAdapter, "structure")
    mock_call2 = mocker.patch.object(CustomAwkwardAdapter, "read")
    mock_call3 = mocker.patch.object(CustomAwkwardAdapter, "read_buffers")
    mock_call4 = mocker.patch.object(CustomAwkwardAdapter, "write")
    mock_call5 = mocker.patch.object(CustomAwkwardAdapter, "specs")
    mock_call6 = mocker.patch.object(CustomAwkwardAdapter, "metadata")

    structure = AwkwardStructure(length=2, form={"a": "b"})

    metadata: JSON = {"foo": "bar"}
    anyslice = (1, 1, 1)
    container = DirectoryContainer(directory=Path("somedirectory"), form={})
    form_keys = ["a", "b", "c"]

    anyawkwardadapter = CustomAwkwardAdapter(container, structure, metadata=metadata)

    assert anyawkwardadapter.structure_family == StructureFamily.awkward

    awkwardadapter_protocol_functions(anyawkwardadapter, anyslice, form_keys, container)
    mock_call.assert_called_once()
    mock_call2.assert_called_once()
    mock_call3.assert_called_once_with(form_keys)
    mock_call4.assert_called_once_with(container)
    mock_call5.assert_called_once()
    mock_call6.assert_called_once()


class CustomSparseAdapter:
    structure_family: Literal[StructureFamily.sparse] = StructureFamily.sparse

    def __init__(
        self,
        blocks: Dict[Tuple[int, ...], Tuple[NDArray[Any], Any]],
        structure: COOStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """
        Construct from blocks with coords given in block-local reference frame.
        Parameters
        ----------
        blocks :
        structure :
        metadata :
        specs :
        access_policy :
        """
        self.blocks = blocks
        self._metadata = metadata or {}
        self._structure = structure
        self._specs = specs or []
        self.access_policy = access_policy

        all_coords = [[1], [1]]
        all_data = [[1], [1]]

        self.arr = sparse.COO(
            data=numpy.concatenate(all_data),
            coords=numpy.concatenate(all_coords, axis=-1),
            shape=self._structure.shape,
        )

    def structure(self) -> COOStructure:
        return self._structure

    def read(self, slice: NDSlice) -> sparse.COO:
        return self.arr

    def read_block(self, block: Tuple[int, ...]) -> sparse.COO:
        return self.arr

    def specs(self) -> List[Spec]:
        return self._specs

    def metadata(self) -> JSON:
        return self._metadata


def sparseadapter_protocol_functions(
    adapter: SparseAdapter, slice: NDSlice, block: Tuple[int, ...]
) -> None:
    adapter.structure()
    adapter.read(slice)
    adapter.read_block(block)
    adapter.specs()
    adapter.metadata()


def test_sparseadapter_protocol(mocker: MockFixture) -> None:
    mock_call = mocker.patch.object(CustomSparseAdapter, "structure")
    mock_call2 = mocker.patch.object(CustomSparseAdapter, "read")
    mock_call3 = mocker.patch.object(CustomSparseAdapter, "read_block")
    mock_call4 = mocker.patch.object(CustomSparseAdapter, "specs")
    mock_call5 = mocker.patch.object(CustomSparseAdapter, "metadata")

    structure = COOStructure(shape=(2 * 5,), chunks=((5, 5),))

    array = numpy.random.rand(2, 512, 512)
    blocks: Dict[Tuple[int, ...], Tuple[NDArray[Any], Any]] = {(1,): (array, (1,))}
    metadata: JSON = {"foo": "bar"}
    anyslice = (1, 1, 1)
    anyblock = (1, 1, 1)

    anysparseadapter = CustomSparseAdapter(blocks, structure, metadata=metadata)
    assert anysparseadapter.structure_family == StructureFamily.sparse

    sparseadapter_protocol_functions(anysparseadapter, anyslice, anyblock)
    mock_call.assert_called_once()
    mock_call2.assert_called_once_with(anyslice)
    mock_call3.assert_called_once_with(anyblock)
    mock_call4.assert_called_once()
    mock_call5.assert_called_once()


class CustomTableAdapter:
    structure_family: Literal[StructureFamily.table] = StructureFamily.table

    def __init__(
        self,
        partitions: List[Any],
        structure: TableStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """

        Parameters
        ----------
        partitions :
        structure :
        metadata :
        specs :
        access_policy :
        """
        self._metadata = metadata or {}
        self._partitions = list(partitions)
        self._structure = structure
        self._specs = specs or []
        self.access_policy = access_policy

    def structure(self) -> TableStructure:
        return self._structure

    def read(
        self, fields: Optional[List[str]] = None
    ) -> Union[dask.dataframe.DataFrame, pandas.DataFrame]:
        return self._partitions

    def read_partition(
        self,
        partition: int,
        fields: Optional[str] = None,
    ) -> Union[dask.dataframe.DataFrame, pandas.DataFrame]:
        return self._partitions[partition]

    def specs(self) -> List[Spec]:
        return self._specs

    def metadata(self) -> JSON:
        return self._metadata

    def __getitem__(self, key: str) -> ArrayAdapter:
        array = numpy.random.rand(2, 512, 512)
        metadata: JSON = {"foo": "bar"}
        structure = ArrayStructure(
            data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("int32")),
            shape=(2, 512, 512),
            chunks=((1, 1), (512,), (512,)),
            dims=("time", "x", "y"),  # optional
        )
        return CustomArrayAdapter(array, structure, metadata=metadata)


def tableadapter_protocol_functions(
    adapter: TableAdapter, partition: int, fields: List[str]
) -> None:
    adapter.structure()
    adapter.read(fields)
    adapter.read_partition(partition)
    adapter.specs()
    adapter.metadata()
    adapter["abc"]


def test_tableadapter_protocol(mocker: MockFixture) -> None:
    mock_call = mocker.patch.object(CustomTableAdapter, "structure")
    mock_call2 = mocker.patch.object(CustomTableAdapter, "read")
    mock_call3 = mocker.patch.object(CustomTableAdapter, "read_partition")
    mock_call4 = mocker.patch.object(CustomTableAdapter, "specs")
    mock_call5 = mocker.patch.object(CustomTableAdapter, "metadata")
    mock_call6 = mocker.patch.object(CustomTableAdapter, "__getitem__")

    structure = TableStructure(
        arrow_schema="a", npartitions=1, columns=["A"], resizable=False
    )

    partitions = pandas.DataFrame([1])
    metadata: JSON = {"foo": "bar"}
    fields = ["a", "b", "c"]
    partition = 1

    anytableadapter = CustomTableAdapter(partitions, structure, metadata=metadata)
    assert anytableadapter.structure_family == StructureFamily.table

    tableadapter_protocol_functions(anytableadapter, partition, fields)
    mock_call.assert_called_once()
    mock_call2.assert_called_once_with(fields)
    mock_call3.assert_called_once_with(partition)
    mock_call4.assert_called_once()
    mock_call5.assert_called_once()
    mock_call6.assert_called_once_with("abc")


class CustomAccessPolicy:
    ALL = ALL_ACCESS

    def __init__(self, scopes: Optional[Scopes] = None) -> None:
        self.scopes = scopes if (scopes is not None) else ALL_SCOPES

    def _get_id(self, principal: Principal) -> None:
        return None

    def allowed_scopes(self, node: BaseAdapter, principal: Principal) -> Scopes:
        allowed = self.scopes
        somemetadata = node.metadata()  # noqa: 841
        return allowed

    def filters(
        self, node: BaseAdapter, principal: Principal, scopes: Scopes
    ) -> Filters:
        queries: Filters = []
        somespecs = node.specs()  # noqa: 841
        return queries


def accesspolicy_protocol_functions(
    policy: AccessPolicy, node: BaseAdapter, principal: Principal, scopes: Scopes
) -> None:
    policy.allowed_scopes(node, principal)
    policy.filters(node, principal, scopes)


def test_accesspolicy_protocol(mocker: MockFixture) -> None:
    mock_call = mocker.patch.object(CustomAwkwardAdapter, "metadata")
    mock_call2 = mocker.patch.object(CustomAwkwardAdapter, "specs")

    anyaccesspolicy = CustomAccessPolicy(scopes={"a12mdjnk4"})

    structure = AwkwardStructure(length=2, form={"a": "b"})

    metadata: JSON = {"foo": "bar"}
    container = DirectoryContainer(directory=Path("somedirectory"), form={})
    principal = Principal(
        uuid="12345678124123412345678123456781", type=PrincipalType.user
    )
    scopes = {"abc"}

    anyawkwardadapter = CustomAwkwardAdapter(container, structure, metadata=metadata)

    accesspolicy_protocol_functions(
        anyaccesspolicy, anyawkwardadapter, principal, scopes
    )
    mock_call.assert_called_once()
    mock_call2.assert_called_once()
