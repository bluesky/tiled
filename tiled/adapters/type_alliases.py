from typing import TypedDict

JSON = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None
Spec = TypedDict({"name": str, "version": str})
HDF5 = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None
