from typing import Any, Optional, Text, Iterable

from .util import get_hashable
from .constants import OP_CODE
from .predicate import ConditionalExpression
from .interfaces import OrderingInterface, SymbolicAttributeInterface


class SymbolicAttribute(SymbolicAttributeInterface):
    def __init__(self, key: Text, symbol: Optional['Symbol'] = None) -> None:
        from store.ordering import Ordering

        self.ordering_class = Ordering
        self.symbol = symbol
        self.key = key

    def __lt__(self, value: Any) -> ConditionalExpression:
        return ConditionalExpression(OP_CODE.LT, self, get_hashable(value))

    def __gt__(self, value: Any) -> ConditionalExpression:
        return ConditionalExpression(OP_CODE.GT, self, get_hashable(value))

    def __ge__(self, value: Any) -> ConditionalExpression:
        return ConditionalExpression(OP_CODE.GE, self, get_hashable(value))

    def __le__(self, value: Any) -> ConditionalExpression:
        return ConditionalExpression(OP_CODE.LE, self, get_hashable(value))

    def __eq__(self, value: Any) -> ConditionalExpression:
        return ConditionalExpression(OP_CODE.EQ, self, get_hashable(value))

    def __ne__(self, value: Any) -> ConditionalExpression:
        return ConditionalExpression(OP_CODE.NE, self, get_hashable(value))

    def is_one_of(self, value: Iterable) -> ConditionalExpression:
        return ConditionalExpression(OP_CODE.IN, self, get_hashable(value))

    @property
    def asc(self) -> OrderingInterface:
        return self.ordering_class(self, desc=False)

    @property
    def desc(self) -> OrderingInterface:
        return self.ordering_class(self, desc=True)


class Symbol:
    Attribute = SymbolicAttribute

    def __init__(self):
        self.attrs = {}

    def __getattr__(self, key: Text) -> 'SymbolicAttribute':
        if key not in self.attrs:
            attr = SymbolicAttribute(key, symbol=self)
            self.attrs[key] = attr
        return self.attrs[key]