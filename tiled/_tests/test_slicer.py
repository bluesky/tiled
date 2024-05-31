import pytest
from fastapi import Query

from ..server.dependencies import slice_

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
