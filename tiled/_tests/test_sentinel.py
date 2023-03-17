import copy

from tiled.utils import Sentinel


def test_sentinel_self_and_copy_equality_and_identity():
    s = Sentinel("TEST")
    assert s == s
    assert s is s
    assert copy.copy(s) == s
    assert copy.copy(s) is s
    assert copy.deepcopy(s) == s
    assert copy.deepcopy(s) is s
