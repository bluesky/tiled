from typing import List, Optional

import pydantic


class CompositeStructure(pydantic.BaseModel):
    contents: Optional[dict]
    count: Optional[int]
    flat_keys: List[str] = []  # In pydantic, OK without default_factory
