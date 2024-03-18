from collections import OrderedDict

from ..utils import Sentinel

DELETE_KEY = Sentinel("DELETE_KEY")


def apply_update_patch(*objs, **kw):
    # adapted from https://github.com/OpenDataServices/json-merge-patch
    result = objs[0]
    for obj in objs[1:]:
        result = _update_obj(result, obj, kw.get("position"))
    return result


def _update_obj(result, obj, position=None):
    # adapted from https://github.com/OpenDataServices/json-merge-patch
    if not isinstance(result, dict):
        result = OrderedDict() if position else {}

    if not isinstance(obj, dict):
        return obj

    if position:
        if position not in ("first", "last"):
            raise ValueError("position can either be first or last")
        if not isinstance(result, OrderedDict) or not isinstance(obj, OrderedDict):
            raise ValueError("If using position all dicts need to be OrderedDicts")

    for key, value in obj.items():
        if isinstance(value, dict):
            target = result.get(key)
            if isinstance(target, dict):
                _update_obj(target, value, position)
                continue
            result[key] = OrderedDict() if position else {}
            if position and position == "first":
                result.move_to_end(key, False)
            _update_obj(result[key], value, position)
            continue
        if value is DELETE_KEY:
            result.pop(key, None)
            continue
        if key not in result and position == "first":
            result[key] = value
            result.move_to_end(key, False)
        else:
            result[key] = value

    return result
