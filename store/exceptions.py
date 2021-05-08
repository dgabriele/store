"""
Custom Exceptions.
"""

from typing import Optional, Text, Any


class StoreException(Exception):
    """
    Base exception for all others.
    """


class NotHashable(StoreException):
    """
    Raised when a value being inserted into a store field index isn't
    Python-hashable (it must be).
    """

    def __init__(self, value: Any, key: Optional[Text] = None) -> None:
        super().__init__(
            f'cannot store unhashable type: {type(value)}'
            + f' (key: {key})' if key is not None else ''
        )
        self.key = key
        self.value = value


class NotSelectable(StoreException):
    def __init__(self, value: Any) -> None:
        super().__init__(f'object not selectable: {value}')