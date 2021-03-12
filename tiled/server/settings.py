from functools import lru_cache
import importlib
import operator
import os
from typing import Any

import entrypoints
from pydantic import BaseSettings, validator


class Settings(BaseSettings):

    catalog_object: Any = None
    catalog_object_path: str = os.getenv("ROOT_CATALOG")
    allow_anonymous_access: bool = bool(int(os.getenv("ALLOW_ANONYMOUS_ACCESS", True)))
    enable_custom_routers: bool = bool(int(os.getenv("ENABLE_CUSTOM_ROUTERS", True)))
    # dask_scheduler_address : str = os.getenv("DASK_SCHEDULER")

    @validator("catalog_object_path")
    def valid_object_path(cls, value):
        if getattr(cls, "catalog_object", None) is not None:
            raise ValueError(
                "catalog_object and catalog_object_path may not both be set"
            )
        # TODO This could be more precise to catch more error cases.
        if value is None:
            return
        import_path, obj_path = str(value).split(":")
        for token in import_path.split("."):
            if not token.isidentifier():
                raise ValueError("Not a valid import path")
        for token in obj_path.split("."):
            if not token.isidentifier():
                raise ValueError("Not a valid attribute in a module")
        return str(value)

    @validator("catalog_object")
    def mutual_exclusion(cls, value):
        if getattr(cls, "catalog_object_path", None) is not None:
            raise ValueError(
                "catalog_object and catalog_object_path may not both be set"
            )

    @property
    def catalog(self):
        if self.catalog_object_path is not None:
            import_path, obj_path = self.catalog_object_path.split(":")
            module = importlib.import_module(import_path)
            return operator.attrgetter(obj_path)(module)
        else:
            return self.catalog_object


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
