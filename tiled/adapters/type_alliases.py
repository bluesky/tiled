from typing import Dict, List, TypeAlias, Union

JSON: TypeAlias = Dict[
    str, Union[str, int, float, bool, Dict[str, "JSON"], List["JSON"]]
]
