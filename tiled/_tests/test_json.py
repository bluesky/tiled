import numpy
import pytest

from ..utils import safe_json_dump


@pytest.fixture
def example_data():
    example_data = {"test": numpy.array([b"test"], dtype="|S4")}
    return example_data


@pytest.fixture
def example_nested_data():
    nested_data = {
        "data": [
            {
                "id": "test",
                "attributes": {
                    "metadata": {"test": numpy.array([b"test"], dtype="|S4")}
                },
            },
            {},
        ]
    }
    return nested_data


def test_ndarray(example_data):
    safe_json_dump(example_data)


def test_nested_ndarray(example_nested_data):
    safe_json_dump(example_nested_data)
