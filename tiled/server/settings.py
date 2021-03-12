from functools import lru_cache
import os
from typing import Any

import entrypoints
from pydantic import BaseSettings


class Settings(BaseSettings):

    catalog: Any = None
    allow_anonymous_access: bool = bool(int(os.getenv("ALLOW_ANONYMOUS_ACCESS", True)))
    enable_custom_routers: bool = bool(int(os.getenv("ENABLE_CUSTOM_ROUTERS", True)))
    # dask_scheduler_address : str = os.getenv("DASK_SCHEDULER")


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
