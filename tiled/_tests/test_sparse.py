import numpy
import sparse

from ..adapters.mapping import MapAdapter
from ..adapters.sparse import COOAdapter
from ..client import from_tree

N = 3
state = numpy.random.RandomState(0)
a = state.random((N * 2, N * 2))
a[a < 0.5] = 0  # Fill half of the array with zeros.
s = sparse.COO(a)
blocks = {}
for i in range(2):
    for j in range(2):
        chunk = s[i * N : (1 + i) * N, j * N : (1 + j) * N]  # noqa: E203
        # Coordinates should be in the reference frame of the whole array.
        coords = chunk.coords + [[i * N], [j * N]]
        blocks[i, j] = coords, chunk.data
mapping = {
    "single_chunk": COOAdapter.from_coo(sparse.COO(a)),
    "multi_chunk": COOAdapter.from_global_ref(
        blocks=blocks, shape=(2 * N, 2 * N), chunks=((N, N), (N, N))
    ),
}
tree = MapAdapter(mapping)


def test_sparse_single_chunk():
    client = from_tree(tree)
    actual_via_slice = client["single_chunk"][:]
    actual_via_read = client["single_chunk"].read()
    actual_via_read_block = client["single_chunk"].read_block((0, 0))
    assert numpy.array_equal(actual_via_slice.todense(), actual_via_read.todense())
    assert numpy.array_equal(
        actual_via_slice.todense(), actual_via_read_block.todense()
    )
    assert numpy.array_equal(actual_via_slice.todense(), a)


def test_sparse_multi_chunk():
    client = from_tree(tree)
    actual_via_slice = client["multi_chunk"][:]
    actual_via_read = client["multi_chunk"].read()
    assert numpy.array_equal(actual_via_slice.todense(), actual_via_read.todense())
    assert numpy.array_equal(actual_via_slice.todense(), a)
