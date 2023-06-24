import subprocess
from shutil import which

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomHook(BuildHookInterface):
    """
    A custom build hook for Tiled that builds the React UI in share/tiled
    """

    def initialize(self, version, build_data):
        npm_path = which("npm")
        if npm_path is None:
            raise ValueError
        subprocess.check_call([npm_path, "install"], cwd="./web-frontend")
        subprocess.check_call([npm_path, "run", "build:pydist"], cwd="./web-frontend")
