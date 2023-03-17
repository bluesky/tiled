import copy

from tiled.utils import Sentinel


def test_sentinel_self_and_copy_equality():
    s = Sentinel("TEST")
    assert s == s
    assert copy.copy(s) == s
    assert copy.deepcopy(s) == s
