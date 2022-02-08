import numpy
import pytest

from ..utils import safe_json_dump


@pytest.fixture
def example_data():
    example_data = {"test": numpy.array([0], dtype="i8")}
    return example_data


@pytest.fixture
def example_bytes_data():
    example_bytes_data = {"test": numpy.array([b"test"], dtype="|S4")}
    return example_bytes_data


@pytest.fixture
def example_nested_data():
    nested_data = {
        "data": [
            {
                "id": "test",
                "attributes": {"metadata": {"test": numpy.array([0], dtype="i8")}},
            },
            {},
        ]
    }
    return nested_data


@pytest.fixture
def example_nested_bytes_data():
    nested_bytes_data = {
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
    return nested_bytes_data


def test_ndarray(example_data):
    safe_json_dump(example_data)


def test_bytes_ndarray(example_bytes_data):
    safe_json_dump(example_bytes_data)


def test_nested_ndarray(example_nested_data):
    safe_json_dump(example_nested_data)


def test_nested_bytes_ndarray(example_nested_bytes_data):
    safe_json_dump(example_nested_bytes_data)
