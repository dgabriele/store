"""
Misc. functions.
"""

from typing import Any
from collections.abc import Hashable


def is_hashable(obj: Any) -> bool:
    """
    Return True if object is hashable, according to Python.
    """
    return isinstance(obj, Hashable)