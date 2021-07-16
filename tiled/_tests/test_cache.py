import numpy
import pytest

from ..readers.array import ArrayAdapter
from ..client import from_tree
from ..client.cache import Cache, download
from ..trees.in_memory import Tree


tree = Tree(
    {"arr": ArrayAdapter.from_array(numpy.arange(10), metadata={"a": 1})},
    metadata={"t": 1},
)


@pytest.fixture(params=["in_memory", "on_disk"])
def cache(request, tmpdir):
    if request.param == "in_memory":
        return Cache.in_memory(2e9)
    if request.param == "on_disk":
        return Cache.on_disk(tmpdir, available_bytes=2e9)


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_integration(cache, structure_clients):
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    expected_tree_md = client.metadata
    expected_arr = client["arr"][:]
    if structure_clients == "dask":
        expected_arr = expected_arr.compute()
    expected_arr_md = client["arr"].metadata
    client["arr"]  # should be a cache hit
    client.offline = True
    actual_tree_md = client.metadata
    actual_arr = client["arr"][:]
    if structure_clients == "dask":
        actual_arr = actual_arr.compute()
    actual_arr_md = client["arr"].metadata
    assert numpy.array_equal(actual_arr, expected_arr)
    assert expected_arr_md == actual_arr_md
    assert expected_tree_md == actual_tree_md


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_download(cache, structure_clients):
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    download(client)
    client.offline = True
    # smoke test
    client.metadata
    arr = client["arr"][:]
    if structure_clients == "dask":
        arr.compute()
    client["arr"].metadata
