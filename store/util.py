"""
Misc. functions.
"""

import inspect

from typing import Any, Dict
from collections.abc import Hashable

from .exceptions import NotHashable


def is_hashable(obj: Any) -> bool:
    """
    Return True if object is hashable, according to Python.
    """
    return isinstance(obj, Hashable)


def get_hashable(value: Any, return_exc=False) -> Any:
    """
    Some datatypes, like dicts and sets, are not hashable and can't be
    inserted into index dicts as keys; therefore, we must convert them to a
    form that is. That's what we do here.
    """
    if isinstance(value, dict):
        return tuple(sorted(value.items()))
    elif isinstance(value, (set, list)):
        return tuple(sorted(value))
    else:
        exc = NotHashable(
            f'cannot store unhashable types, but got {type(value)}'
        )
        if not is_hashable(value):
            if not return_exc:
                raise exc
            else:
                return exc
        return value


def to_dict(obj: Any) -> Dict:
    """
    Convert an instance object into a dict, taking all hashable attributes,
    whether class or instance attributes.
    """
    data = {}
    predicate = lambda x: not callable(x)

    for key, val in inspect.getmembers(obj, predicate=predicate):
        if not key.startswith('_'):
            # only accept hashable (or forcably hashable) values
            hashable_val = get_hashable(val, return_exc=True)
            if not isinstance(hashable_val, NotHashable):
                data[key] = val

    return data