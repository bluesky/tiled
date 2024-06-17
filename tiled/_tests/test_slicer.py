import pathlib

import pytest
from fastapi import Query
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.server.dependencies import slice_


@pytest.fixture(scope="module")
def module_tmp_path(tmp_path_factory: pytest.TempdirFactory) -> pathlib.Path:
    return tmp_path_factory.mktemp("temp")


@pytest.fixture(scope="module")
def client(module_tmp_path):
    catalog = in_memory(writable_storage=module_tmp_path)
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        client.write_array([1], key="x")
        yield client


slice_test_data = [
    "",
    ":",
    "::",
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
    ",",
    ",,",
    ",:",
    ":,::",
    ",,:,::,,::,:,,::,",
    "0,1,2",
    "5:,:10,::-5",
    "1:2:3,4:5:6,7:8:9",
    "10::20,30::40,50::60",
    "1 : 2",
    "1:2, 3",
    "1 ,2:3",
    "1 , 2 , 3",
]

slice_typo_data = [
    ":::",
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
    assert slice_(slice) == reference_slice_(slice)


@pytest.mark.parametrize("slice", slice_typo_data)
def test_slicer_typo_data(slice: str):
    """
    Test the slicer function with invalid input
    """
    with pytest.raises(TypeError):
        _ = slice_(slice)


@pytest.mark.parametrize("slice", slice_malicious_data)
def test_slicer_malicious_exec(slice: str):
    """
    Test the slicer function with 'malicious' input
    """
    with pytest.raises(ValueError):
        _ = slice_(slice)


@pytest.mark.parametrize("slice_", slice_typo_data + slice_malicious_data)
def test_slicer_fastapi_query_rejection(slice_, client):
    http_client = client.context.http_client
    response = http_client.get(f"/api/v1/array/block/x?block=0&slice={slice_}")
    assert response.status_code == HTTP_422_UNPROCESSABLE_ENTITY
