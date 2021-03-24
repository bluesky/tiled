from functools import lru_cache
import os
from typing import Any, List

import entrypoints
from pydantic import BaseSettings


class Settings(BaseSettings):

    catalog: Any = None
    allow_anonymous_access: bool = bool(
        int(os.getenv("TILED_ALLOW_ANONYMOUS_ACCESS", True))
    )
    allow_origins: List[str] = [
        item for item in os.getenv("TILED_ALLOW_ORIGINS", "").split() if item
    ]
    enable_custom_routers: bool = bool(
        int(os.getenv("TILED_ENABLE_CUSTOM_ROUTERS", True))
    )
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
