from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from tiled.server.dependencies import get_root_tree


def test_get_tree():
    req = Mock()
    req.app.state.root_tree = "test_tree"

    assert get_root_tree(req) == "test_tree"


def test_missing_tree():
    req = Mock()
    del req.app.state.root_tree

    with pytest.raises(HTTPException):
        get_root_tree(req)
