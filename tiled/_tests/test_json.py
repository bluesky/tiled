import numpy
import pytest

from ..utils import safe_json_dump


@pytest.mark.parametrize(
    "test_params",
    argvalues=[
        {"array": numpy.array([0], dtype="i8")},
        {"array": numpy.array([b"test"], dtype="|S4")},
    ],
)
def test_ndarray(test_params):
    safe_json_dump(test_params["array"])


@pytest.mark.parametrize(
    "test_params",
    argvalues=[
        {"array": numpy.array([0], dtype="i8")},
        {"array": numpy.array([b"test"], dtype="|S4")},
    ],
)
def test_nested_ndarray(test_params):
    example_nested_data = {
        "data": [
            {
                "id": "test",
                "attributes": {"metadata": {"test": test_params["array"]}},
            },
            {},
        ]
    }
    safe_json_dump(example_nested_data)
