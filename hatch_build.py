import os
import subprocess
import sys
from shutil import which

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomHook(BuildHookInterface):
    """
    A custom build hook for Tiled that builds the React UI in share/tiled
    """

    def initialize(self, version, build_data):
        # https://hatch.pypa.io/1.1/plugins/build-hook/#hatchling.builders.hooks.plugin.interface.BuildHookInterface

        # Hatchling intends for us to mutate the input build_data communicate
        # that 'share/tiled/ui' contains build artifacts that should be included
        # in the distribution.

        # Set this irrespective of whether the build happens below. It may have
        # already been done manually by the user. This simply allow-lists the
        # files, however they were put there.
        build_data["artifacts"].append("share/tiled/ui")

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
            return
        print(
            f"Building Tiled web UI using {npm_path!r}. (Set TILED_BUILD_SKIP_UI=1 to skip.)",
            file=sys.stderr,
        )
        subprocess.check_call([npm_path, "install"], cwd="./web-frontend")
        subprocess.check_call([npm_path, "run", "build:pydist"], cwd="./web-frontend")
