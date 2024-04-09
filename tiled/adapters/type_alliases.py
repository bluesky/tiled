# JSON = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None
# from typing import Any
from typing import Any, Dict, List, Union

# JSON = dict[str, "JSON"] | list["JSON"]
JSONValue = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
JSON = Union[Dict[str, JSONValue], List[JSONValue]]
