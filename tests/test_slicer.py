import builtins
import pathlib

import numpy as np
import pytest
from fastapi import Query
from hypothesis import assume, given
from hypothesis import strategies as st
from starlette.status import HTTP_422_UNPROCESSABLE_CONTENT

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.ndslice import NDSlice, compose_slices
from tiled.server.app import build_app


@pytest.fixture(scope="module")
def module_tmp_path(tmp_path_factory: pytest.TempdirFactory) -> pathlib.Path:
    return tmp_path_factory.mktemp("temp")


@pytest.fixture(scope="module")
def client(module_tmp_path):
    catalog = in_memory(writable_storage=str(module_tmp_path))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        client.write_array([1], key="x")
        yield client


slice_test_data = [
    "",
    ":",
    "::",
    "...",
    ":,::",
    "0",
    "0:",
    "0::",
    ":0",
    "::0",
    "5:",
    ":10",
    "::12",
    "-1",
    "-5:",
    ":-5",
    "::-45",
    "3:5",
    "5:3",
    "123::4",
    "5::678",
    ":123:4",
    ":5:678",
    "0,1,2",
    "5:,:10,::-5",
    "1:2:3,4:5:6,7:8:9",
    "10::20,30::40,50::60",
    "1 : 2",
    "1:2, 3",
    "1 ,2:3",
    "1 , 2 , 3",
]

slice_missing_data = [
    ",",
    ",,",
    ",:",
    ",,:,::,,::,:,,::,",
    ",1:2,,:3:4",
    ",:1, 1::, ,, 1:5:2" "-2:4:1,,:5:2",
]

slice_typo_data = [
    ":::",
    "..",
    "....",
    "1:2:3:4",
    "1:2,3:4:5:6",
]

slice_malicious_data = [
    "1:(2+3)",
    "1**2",
    "print('oh so innocent')",
    "; print('oh so innocent')",
    ")\"; print('oh so innocent')",
    "1:2)\"; print('oh so innocent')",
    "1:2)\";print('oh_so_innocent')",
    "import sys; sys.exit()",
    "; import sys; sys.exit()",
    "touch /tmp/x",
    "rm -rf /tmp/*",
]


# this is the outgoing slice_ function from tiled.server.dependencies as is
def reference_slice_(
    slice: str = Query(None, pattern="^[-0-9,:]*$"),
):
    "Specify and parse a block index parameter."
    import numpy

    # IMPORTANT We are eval-ing a user-provider string here so we need to be
    # very careful about locking down what can be in it. The regex above
    # excludes any letters or operators, so it is not possible to execute
    # functions or expensive arithmetic.
    return tuple(
        [
            eval(f"numpy.s_[{dim!s}]", {"numpy": numpy})
            for dim in (slice or "").split(",")
            if dim
        ]
    )


@pytest.mark.parametrize("slice", slice_test_data)
def test_slicer(slice: str):
    """
    Test the slicer function
    """
    assert NDSlice.from_numpy_str(slice) == reference_slice_(slice)


@pytest.mark.parametrize("slice", slice_typo_data + slice_missing_data)
def test_slicer_typo_data(slice: str):
    """
    Test the slicer function with invalid input
    """
    with pytest.raises(ValueError):
        _ = NDSlice.from_numpy_str(slice)


@pytest.mark.parametrize("slice", slice_malicious_data)
def test_slicer_malicious_exec(slice: str):
    """
    Test the slicer function with 'malicious' input
    """
    with pytest.raises(ValueError):
        _ = NDSlice.from_numpy_str(slice)


@pytest.mark.parametrize("slice_", slice_typo_data + slice_malicious_data)
def test_slicer_fastapi_query_rejection(slice_, client):
    http_client = client.context.http_client
    response = http_client.get(f"/api/v1/array/block/x?block=0&slice={slice_}")
    assert response.status_code == HTTP_422_UNPROCESSABLE_CONTENT


slice_cases = [
    ("[0:5:2]", (slice(0, 5, 2),), [{"start": 0, "stop": 5, "step": 2}]),
    ("(1:3)", (slice(1, 3),), [{"start": 1, "stop": 3}]),
    ("1:", (slice(1, None),), [{"start": 1}]),
    ("[0:3]", (slice(0, 3),), [{"start": 0, "stop": 3}]),
    ("[:]", (slice(None, None, None),), [{}]),
    ("[...]", (Ellipsis,), [{}]),
    ("::2", (slice(None, None, 2),), [{"step": 2}]),
    ("[1::2]", (slice(1, None, 2),), [{"start": 1, "step": 2}]),
    ("[1 : : 2]", (slice(1, None, 2),), [{"start": 1, "step": 2}]),
    ("[:3:2]", (slice(None, 3, 2),), [{"stop": 3, "step": 2}]),
    ("[1:3:]", (slice(1, 3),), [{"start": 1, "stop": 3}]),
    (
        "[1:5:2, ..., :5]",
        (slice(1, 5, 2), Ellipsis, slice(None, 5)),
        [{"start": 1, "stop": 5, "step": 2}, {}, {"stop": 5}],
    ),
]


@pytest.mark.parametrize("as_string, as_tuple, as_json", slice_cases)
def test_ndslice_conversion(as_string, as_tuple, as_json):
    assert NDSlice.from_numpy_str(as_string) == as_tuple

    # JSON does not preserve Ellipsis expanding them to empty slices
    as_tuple_with_ellipsis = tuple(
        slice(None) if x is Ellipsis else x for x in as_tuple
    )
    assert NDSlice.from_json(as_json) == as_tuple_with_ellipsis

    # Normalize the string representations before comparing them
    norm_string = as_string.strip("(][)").replace(" ", "").lstrip("0")
    norm_string = "1:3" if norm_string == "1:3:" else norm_string
    assert NDSlice(*as_tuple).to_numpy_str() == norm_string


slice_json_cases = [
    ((slice(0, 5, 2),), [{"start": 0, "stop": 5, "step": 2}], None),
    ((slice(0, 5, 2),), [{"start": 0, "stop": 5, "step": 2}], 1),
    ((slice(0, 5, 2), slice(None)), [{"start": 0, "stop": 5, "step": 2}, {}], 2),
    ((slice(0, 5, 2),), [{"start": 0, "stop": 5, "step": 2}, {}, {}], 3),
    ((slice(0, 5, 2), Ellipsis), [{"start": 0, "stop": 5, "step": 2}, {}, {}], 3),
    ((Ellipsis, slice(0, 5, 2)), [{}, {}, {"start": 0, "stop": 5, "step": 2}], 3),
    (
        (slice(0, 5, 2), Ellipsis, slice(-10, -20, -1)),
        [
            {"start": 0, "stop": 5, "step": 2},
            {},
            {},
            {},
            {"start": -10, "stop": -20, "step": -1},
        ],
        5,
    ),
    (
        (slice(0, 5, 2), Ellipsis, slice(-10, -20, -1)),
        [{"start": 0, "stop": 5, "step": 2}, {"start": -10, "stop": -20, "step": -1}],
        2,
    ),
]


@pytest.mark.parametrize("as_tuple, as_json, ndim", slice_json_cases)
def test_ndslice_to_json(as_tuple, as_json, ndim):
    assert NDSlice(*as_tuple).to_json(ndim=ndim) == as_json


def test_errors_in_slices():
    # Multiple Ellipsis are not allowed
    with pytest.raises(ValueError):
        NDSlice(Ellipsis, slice(1), Ellipsis)

    # Unspecified number of dimensions when converting to JSON
    with pytest.raises(ValueError):
        NDSlice(Ellipsis, slice(1)).to_json()

    # Dimension mismatch when converting to JSON
    with pytest.raises(ValueError):
        NDSlice(slice(1), slice(2), slice(3)).to_json(ndim=2)
    with pytest.raises(ValueError):
        NDSlice(slice(1), slice(2), Ellipsis).to_json(ndim=1)


def slice_strategy(dim_len):
    "Generate a Python slice that is valid for arrays up to dim_len."
    return st.builds(
        slice,
        st.one_of(st.none(), st.integers(-dim_len, dim_len)),
        st.one_of(st.none(), st.integers(-dim_len, dim_len)),
        st.one_of(st.none(), st.integers(-3, 3).filter(lambda x: x != 0)),
    ).filter(
        lambda s: sum(
            1
            for _ in range(
                s.start or 0, s.stop if s.stop is not None else dim_len, s.step or 1
            )
        )
    )  # Remove empty slices


def index_strategy(dim_len):
    "Generate either an int index or a slice."
    return st.one_of(
        st.integers(-dim_len, dim_len - 1),
        slice_strategy(dim_len),
    )


def ndslice_strategy(shape):
    "Generate an N-dimensional slices (tuples of indices/slices/Ellipsis) for a given shape"

    # Generate a fully explicit slice with no Ellipsis
    def full_explicit(shape=shape):
        return st.tuples(*(index_strategy(dim_len) for dim_len in shape))

    # Case 1: No Ellipsis, possibly shorter, keep only the first k dimensions
    def without_ellipsis():
        k = st.integers(min_value=1, max_value=len(shape))
        return k.flatmap(lambda k: full_explicit(shape[:k]))

    # Case 2: With Ellipsis
    def with_ellipsis():
        if len(shape) == 0:
            return full_explicit()  # nothing to ellipsize

        el_start = st.integers(0, len(shape) - 1)
        el_stop = st.integers(0, len(shape) - 1)

        return full_explicit().flatmap(
            lambda slc: st.tuples(el_start, el_stop).map(
                lambda se: _insert_ellipsis(slc, *se)
            )
        )

    def _insert_ellipsis(slc, start, stop):
        # Normalize order
        if start > stop:
            start, stop = stop, start

        # Replace [start:stop] with Ellipsis
        return slc[:start] + (Ellipsis,) + slc[stop + 1 :]  # noqa: E203

    return st.one_of(without_ellipsis(), with_ellipsis())


@given(
    shape=st.lists(st.integers(1, 6), min_size=1, max_size=4),
    data=st.data(),
)
def test_compose_slices(shape, data):
    arr = np.arange(np.prod(shape)).reshape(shape)

    # Remove any Ellipsis for the first slice to ensure arr[slc1] is valid
    slc1 = NDSlice(data.draw(ndslice_strategy(shape)))
    slc1 = slc1.expand_for_shape(shape)
    assume(all(s.start != s.stop for s in slc1 if isinstance(s, builtins.slice)))

    # Build the second slice ensuring it is valid for the remaining shape
    shape_after_slc1 = slc1.shape_after_slice(shape)
    assume(shape_after_slc1)  # Skip if the first slice results in an empty array
    slc2 = NDSlice(data.draw(ndslice_strategy(shape_after_slc1)))

    # Apply slicing sequentially
    arr1 = arr[slc1]
    arr2 = arr1[slc2]

    # Now compose the slices and apply once
    slc_composed = compose_slices(slc1, slc2)
    arr_composed = arr[tuple(slc_composed)]
    np.testing.assert_array_equal(arr2, arr_composed)

    # Test the composition via __getitem__
    np.testing.assert_array_equal(arr[slc1][slc2], arr[slc1[slc2]])
