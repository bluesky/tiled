from typing import Any, List, Literal, Optional, Tuple

import numpy
from numpy.typing import NDArray
from pytest_mock import MockType

from tiled.adapters.protocols import AccessPolicy, ArrayAdapter
from tiled.adapters.type_alliases import JSON, NDSlice
from tiled.structures.array import ArrayStructure, BuiltinDtype
from tiled.structures.core import Spec, StructureFamily


class MyArrayAdapter:
    structure_family = Literal[StructureFamily.array]

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


def arrayadapter_protocol_structure(adapter: ArrayAdapter) -> None:
    adapter.structure()


def arrayadapter_protocol_read(adapter: ArrayAdapter, slice: NDSlice) -> None:
    adapter.read(slice)


def arrayadapter_protocol_read_block(
    adapter: ArrayAdapter, block: Tuple[int, ...]
) -> None:
    adapter.read_block(block)


def arrayadapter_protocol_specs(adapter: ArrayAdapter) -> None:
    adapter.specs()


def arrayadapter_protocol_metadata(adapter: ArrayAdapter) -> None:
    adapter.metadata()


def test_arrayadapter_protocol(mocker: MockType) -> None:
    mock_call = mocker.patch.object(MyArrayAdapter, "structure")
    mock_call2 = mocker.patch.object(MyArrayAdapter, "read")
    mock_call3 = mocker.patch.object(MyArrayAdapter, "read_block")
    mock_call4 = mocker.patch.object(MyArrayAdapter, "specs")
    mock_call5 = mocker.patch.object(MyArrayAdapter, "metadata")

    structure = ArrayStructure(
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("int32")),
        shape=(2, 512, 512),
        chunks=((1, 1), (512,), (512,)),
        dims=("time", "x", "y"),  # optional
    )

    array = numpy.random.rand(2, 512, 512)
    metadata: JSON = {"foo": "bar"}
    myslice = (1, 1, 1)
    myblock = (1, 1, 1)

    anyarrayadapter = MyArrayAdapter(array, structure, metadata=metadata)

    arrayadapter_protocol_structure(anyarrayadapter)
    mock_call.assert_called_once()

    arrayadapter_protocol_read(anyarrayadapter, myslice)
    mock_call2.assert_called_once_with(myslice)

    arrayadapter_protocol_read_block(anyarrayadapter, myblock)
    mock_call3.assert_called_once_with(myblock)

    arrayadapter_protocol_specs(anyarrayadapter)
    mock_call4.assert_called_once()

    arrayadapter_protocol_metadata(anyarrayadapter)
    mock_call5.assert_called_once()
