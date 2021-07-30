from os import path
from setuptools import setup, find_packages
import sys
import versioneer


# NOTE: This file must remain Python 2 compatible for the foreseeable future,
# to ensure that we error out properly for people with outdated setuptools
# and/or pip.
min_version = (3, 7)
if sys.version_info < min_version:
    error = """
{{ cookiecutter.package_dist_name }} does not support Python {0}.{1}.
Python {2}.{3} and above is required. Check your Python version like so:

python3 --version

This may be due to an out-of-date pip. Make sure you have pip >= 9.0.1.
Upgrade pip like so:

pip install --upgrade pip
""".format(
        *(sys.version_info[:2] + min_version)
    )
    sys.exit(error)

here = path.abspath(path.dirname(__file__))


with open(path.join(here, "README.md"), encoding="utf-8") as readme_file:
    readme = readme_file.read()


def read_requirements(filename):
    with open(path.join(here, filename)) as requirements_file:
        # Parse requirements.txt, ignoring any commented-out lines.
        requirements = [
            line
            for line in requirements_file.read().splitlines()
            if not line.startswith("#")
        ]
    return requirements


categorized_requirements = {
    key: read_requirements(f"requirements-{key}.txt")
    for key in [
        "client",
        "compression",
        "formats",
        "server",
        "array",
        "dataframe",
        "xarray",
    ]
}
extras_require = {}
extras_require["client"] = sorted(
    set(
        sum(
            (
                categorized_requirements[k]
                for k in ["client", "array", "dataframe", "xarray", "compression"]
            ),
            [],
        )
    )
)
extras_require["server"] = sorted(
    set(
        sum(
            (
                categorized_requirements[k]
                for k in ["server", "array", "dataframe", "xarray", "compression"]
            ),
            [],
        )
    )
)
extras_require["minimal-client"] = categorized_requirements["client"]
extras_require["minimal-server"] = categorized_requirements["server"]
extras_require["formats"] = categorized_requirements["formats"]
extras_require["all"] = extras_require["complete"] = sorted(
    set(sum(categorized_requirements.values(), []))
)

setup(
    name="tiled",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Tile-based access to SciPy/PyData data structures over the web in many formats",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Bluesky Collaboration",
    author_email="dallan@bnl.gov",
    url="https://github.com/bluesky/tiled",
    python_requires=">={}".format(".".join(str(n) for n in min_version)),
    install_requires=[],  # Requirements depend on use case (e.g. client vs server).
    extras_require=extras_require,
    packages=find_packages(exclude=["docs", "tests"]),
    entry_points={
        "console_scripts": [
            "tiled = tiled.commandline.main:main",
        ]
    },
    package_data={
        "tiled": [
            # When adding files here, remember to update MANIFEST.in as well,
            # or else they will not be included in the distribution on PyPI!
            "schemas/*.yml",
        ]
    },
    license="BSD (3-clause)",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
    ],
)
