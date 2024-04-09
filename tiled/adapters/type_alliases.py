from typing import Dict, List, Union

JSON = Dict[str, Union[str, int, float, bool, Dict[str, "JSON"], List["JSON"]]]
