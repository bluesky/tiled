"""
importing tiled.client should not import any heavy optional dependencies.
"""
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "module",
    [
        "numpy",
        "pandas",
        "xarray",
    ],
)
def test_no_heavy_imports(module):
    code = f"import tiled.client; import sys; assert '{module}' not in sys.modules"
    subprocess.check_call([sys.executable, "-c", code])
