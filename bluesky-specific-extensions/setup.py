from os import path
import setuptools

here = path.abspath(path.dirname(__file__))


def read_requirements(filename):
    with open(path.join(here, filename)) as requirements_file:
        # Parse requirements.txt, ignoring any commented-out lines.
        requirements = [
            line
            for line in requirements_file.read().splitlines()
            if not line.startswith("#")
        ]
    return requirements


extras_require = {
    key: read_requirements(f"requirements-{key}.txt") for key in ["client", "server"]
}
extras_require["complete"] = sorted(set(sum(extras_require.values(), [])))


setuptools.setup(
    name="bluesky_catalog",
    entry_points={
        "catalog_server.special_client": [
            "BlueskyRun = bluesky_catalog.client:BlueskyRun",
            "BlueskyEventStream = bluesky_catalog.client:BlueskyEventStream",
        ],
        "catalog_server.custom_routers": ["documents = bluesky_catalog.server:router"],
    },
    extras_require=extras_require,
)
