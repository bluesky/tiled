from setuptools import setup

setup(
    name="bluesky_catalog",
    entry_points={
        "catalog_server.special_client": [
            "BlueskyRun = bluesky_catalog.client:BlueskyRun",
        ],
        "catalog_server.custom_routers": ["documents = bluesky_catalog.server:router"],
    },
    extras_require={"server": ["fastapi", "starlette", "pymongo"]},
)
