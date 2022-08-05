import numpy
import sparse

from ..adapters.mapping import MapAdapter
from ..adapters.sparse import COOAdapter
from ..client import from_tree

N, M = 3, 5
state = numpy.random.RandomState(0)
a = state.random((N * 2, M * 2))
a[a < 0.5] = 0  # Fill half of the array with zeros.
s = sparse.COO(a)
blocks = {}
for i in range(2):
    for j in range(2):
        chunk = s[i * N : (1 + i) * N, j * M : (1 + j) * M]  # noqa: E203
        # Below we test the COOAdapter.from_global_ref constructor, so
        # coordinates should be in the reference frame of the whole array.
        coords = chunk.coords + [[i * N], [j * M]]
        blocks[i, j] = coords, chunk.data
chunks = ((N, N), (M, M))
dims = ["x", "y"]
mapping = {
    "single_chunk": COOAdapter.from_coo(sparse.COO(a), dims=dims),
    "multi_chunk": COOAdapter.from_global_ref(
        blocks=blocks, shape=(2 * N, 2 * M), chunks=chunks, dims=dims
    ),
}
tree = MapAdapter(mapping)


def test_sparse_single_chunk():
    client = from_tree(tree)
    sc = client["single_chunk"]
    actual_via_slice = sc[:]
    actual_via_read = sc.read()
    actual_via_todense = sc.todense()
    actual_via_read_block = sc.read_block((0, 0))
    assert numpy.array_equal(actual_via_slice.todense(), actual_via_read.todense())
    assert numpy.array_equal(
        actual_via_slice.todense(), actual_via_read_block.todense()
    )
    assert numpy.array_equal(actual_via_slice.todense(), a)
    assert numpy.array_equal(actual_via_todense, a)
    assert sc.shape == a.shape
    assert sc.ndim == a.ndim
    assert sc.chunks == tuple((i,) for i in a.shape)
    assert sc.dims == dims


def test_sparse_multi_chunk():
    client = from_tree(tree)
    sc = client["multi_chunk"]
    actual_via_slice = sc[:]
    actual_via_read = sc.read()
    actual_via_todense = sc.todense()
    assert numpy.array_equal(actual_via_slice.todense(), actual_via_read.todense())
    assert numpy.array_equal(actual_via_slice.todense(), a)
    assert numpy.array_equal(actual_via_todense, a)
    assert sc.shape == a.shape
    assert sc.ndim == a.ndim
    assert sc.chunks == chunks
    assert sc.dims == dims
