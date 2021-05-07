from typing import Optional, Text, Any


class BaseException(Exception):
    pass


class NotHashable(BaseException):
    def __init__(self, value: Any, key: Optional[Text] = None) -> None:
        super().__init__(
            f'cannot store unhashable type: {type(obj)}'
            + f' (key: {key})' if key is not None else ''
        )
        self.key = key
        self.value = value