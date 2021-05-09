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
    """
    Exception raised when an unrecognized object is passed into query.select().
    """

    def __init__(self, value: Any) -> None:
        super().__init__(f'object not selectable: {value}')


class NotOrderable(StoreException):
    """
    Exception raised when a type that cannot be sorted is used in an query's
    order_by expression.
    """
    
    def __init__(self, key: Text, value: Any) -> None:
        super().__init__(f'{type(value)} cannot be ordered (key: {key})')