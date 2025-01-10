from typing import Optional

import pydantic


class ContainerStructure(pydantic.BaseModel):
    contents: Optional[dict]
    count: Optional[int]
