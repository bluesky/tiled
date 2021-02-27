from os import path
import setuptools

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "requirements.txt")) as requirements_file:
    # Parse requirements.txt, ignoring any commented-out lines.
    requirements = [
        line
        for line in requirements_file.read().splitlines()
        if not line.startswith("#")
    ]

setuptools.setup(
    name="catalog_server",
    install_requires=requirements,
    packages=setuptools.find_packages(where=".", exclude=["doc", ".ci"]),
)
