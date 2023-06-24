import os
import subprocess
import sys
from pathlib import Path
from shutil import which

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomHook(BuildHookInterface):
    """
    A custom build hook for Tiled that builds the React UI in share/tiled
    """

    def initialize(self, version, build_data):
        if os.getenv("TILED_BUILD_SKIP_UI"):
            print(
                "Will skip building the Tiled web UI because TILED_BUILD_SKIP_UI is set",
                file=sys.stderr,
            )
            return
        npm_path = which("npm")
        if npm_path is None:
            print(
                "Will skip building the Tiled web UI because 'npm' executable is not found",
                file=sys.stderr,
            )
        subprocess.check_call([npm_path, "install"], cwd="./web-frontend")
        subprocess.check_call([npm_path, "run", "build:pydist"], cwd="./web-frontend")
        here = Path(__file__).parent
        assert (here / "share" / "tiled" / "ui").exists()
