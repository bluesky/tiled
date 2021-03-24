from functools import lru_cache
import os
from typing import Any, List

from pydantic import BaseSettings


class Settings(BaseSettings):

    catalog: Any = None
    allow_anonymous_access: bool = bool(
        int(os.getenv("TILED_ALLOW_ANONYMOUS_ACCESS", True))
    )
    allow_origins: List[str] = [
        item for item in os.getenv("TILED_ALLOW_ORIGINS", "").split() if item
    ]


@lru_cache()
def get_settings():
    return Settings()
