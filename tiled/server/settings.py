from functools import lru_cache
import importlib
import operator
import os

import entrypoints
from pydantic import BaseSettings, validator


_DEMO_DEFAULT_ROOT_CATALOG = (
    "tiled.examples.generic:nested_with_access_control"
)


class Settings(BaseSettings):

    catalog_object_path: str = os.getenv("ROOT_CATALOG", _DEMO_DEFAULT_ROOT_CATALOG)
    allow_anonymous_access: bool = bool(int(os.getenv("ALLOW_ANONYMOUS_ACCESS", True)))
    enable_custom_routers: bool = bool(int(os.getenv("ENABLE_CUSTOM_ROUTERS", True)))
    # dask_scheduler_address : str = os.getenv("DASK_SCHEDULER")

    @validator("catalog_object_path")
    def valid_object_path(cls, value):
        # TODO This could be more precise to catch more error cases.
        import_path, obj_path = str(value).split(":")
        for token in import_path.split("."):
            if not token.isidentifier():
                raise ValueError("Not a valid import path")
        for token in obj_path.split("."):
            if not token.isidentifier():
                raise ValueError("Not a valid attribute in a module")
        return str(value)

    @property
    def catalog(self):
        import_path, obj_path = self.catalog_object_path.split(":")
        module = importlib.import_module(import_path)
        return operator.attrgetter(obj_path)(module)


@lru_cache()
def get_settings():
    return Settings()


def get_custom_routers():
    if get_settings().enable_custom_routers:
        return [
            entrypoint.load()
            for entrypoint in entrypoints.get_group_all("tiled.custom_routers")
        ]
    else:
        return []
