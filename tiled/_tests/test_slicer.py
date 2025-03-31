import pathlib

import pytest
from fastapi import Query
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from ..catalog import in_memory
from ..client import Context, from_context
from ..ndslice import NDSlice
from ..server.app import build_app


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
    assert NDSlice.from_query(slice) == reference_slice_(slice)


@pytest.mark.parametrize("slice", slice_typo_data + slice_missing_data)
def test_slicer_typo_data(slice: str):
    """
    Test the slicer function with invalid input
    """
    with pytest.raises(ValueError):
        _ = NDSlice.from_query(slice)


@pytest.mark.parametrize("slice", slice_malicious_data)
def test_slicer_malicious_exec(slice: str):
    """
    Test the slicer function with 'malicious' input
    """
    with pytest.raises(ValueError):
        _ = NDSlice.from_query(slice)


@pytest.mark.parametrize("slice_", slice_typo_data + slice_malicious_data)
def test_slicer_fastapi_query_rejection(slice_, client):
    http_client = client.context.http_client
    response = http_client.get(f"/api/v1/array/block/x?block=0&slice={slice_}")
    assert response.status_code == HTTP_422_UNPROCESSABLE_ENTITY


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
