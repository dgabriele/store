"""
Misc. functions.
"""

import inspect

from typing import Any, Dict, Set, Text, List, Iterable, Union
from collections.abc import Hashable

from ordered_set import OrderedSet

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
        return tuple(sorted(
            (k, get_hashable(v)) for k, v in value.items()
        ))
    elif isinstance(value, list):
        return tuple(get_hashable(x) for x in value)
    elif isinstance(value, set):
        return tuple(sorted(get_hashable(x) for x in value))
    else:
        exc = NotHashable(
            f'cannot store unhashable types, but got {type(value)}'
        )
        if not is_hashable(value):
            if not return_exc:
                raise exc
            return exc
        return value


def to_dict(obj: Any) -> Dict:
    """
    Convert an instance object into a dict, taking all hashable attributes,
    whether class or instance attributes.
    """
    if isinstance(obj, dict):
        return obj

    data = {}
    predicate = lambda x: not callable(x)

    for key, val in inspect.getmembers(obj, predicate=predicate):
        if not key.startswith('_'):
            # only accept hashable (or forcably hashable) values
            hashable_val = get_hashable(val, return_exc=True)
            if not isinstance(hashable_val, NotHashable):
                data[key] = val

    return data


def union(sequences):
    """
    Perform set union
    """
    if sequences:
        if len(sequences) == 1:
            return sequences[0]
        else:
            return set.union(*sequences)
    else:
        return set()


def get_pkeys(
    targets: Iterable[Any], pkey_name: Text, as_set=False
) -> Union[List, OrderedSet]:
    """
    Extract and return "primary keys" from a sequence of objects.
    """
    if as_set:
        pkeys = OrderedSet()
        for target in targets:
            if isinstance(target, dict):
                pkeys.add(target[pkey_name])
            else:
                pkeys.add(getattr(target, pkey_name, target))
        return pkeys
    else:
        pkeys = []
        for target in targets:
            if isinstance(target, dict):
                pkeys.append(target[pkey_name])
            else:
                pkeys.append(getattr(target, pkey_name, target))
        return pkeys