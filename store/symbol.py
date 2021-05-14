"""
class Symbol
"""

from copy import deepcopy
from typing import Any, Optional, Text, Iterable, Type, Dict, Union

from .util import get_hashable
from .constants import OP_CODE
from .predicate import ConditionalExpression
from .interfaces import OrderingInterface, QueryInterface, StateDictInterface, StoreInterface, SymbolicAttributeInterface


class SymbolicAttribute(SymbolicAttributeInterface):
    """
    SymbolicAttribute instances are returned via Symbol.__getattr__. For
    example, doing `symbol.foo` will return a SymbolicAttribute with its key
    attribute having the value "foo". These objects are used in the
    implementation of the Query class.
    """

    def __init__(self, key: Text, symbol: Optional['Symbol'] = None) -> None:
        super().__init__()

        from store.ordering import Ordering

        self.ordering_class: Type[OrderingInterface] = Ordering
        self.symbol = symbol
        self.key = key

    def copy(self) -> 'SymbolicAttribute':
        return type(self)(self.key, self.symbol)

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

    def __deepcopy__(self, memo) -> 'SymbolicAttribute':
        """
        What happens on deepcopy(attr). Note that the symbol attribute is *not*
        deep copied, as most likely, this would result in an infinite loop with
        respect to Symbol's own __deepcopy__ method.
        """
        return type(self)(self.key, self.symbol)

    def one_of(self, value: Iterable) -> ConditionalExpression:
        """
        Example:
        ```
        user = store.symbol()
        query.where(user.email.one_of(subsriber_email_list))
        ```
        """
        return ConditionalExpression(OP_CODE.IN, self, get_hashable(value))

    def not_in(self, value: Iterable) -> ConditionalExpression:
        """
        Example:
        ```
        user = store.symbol()
        query.where(user.email.not_in(subsriber_email_list))
        ```
        """
        return ConditionalExpression(OP_CODE.NOT_IN, self, get_hashable(value))

    @property
    def asc(self) -> OrderingInterface:
        """
        Used like:
        ```
        user = store.symbol()
        query.order_by(user.email.asc, user.created_at.desc)
        ```
        """
        return self.ordering_class(self, desc=False)

    @property
    def desc(self) -> OrderingInterface:
        """
        Used like:
        ```
        user = store.symbol()
        query.order_by(user.email.desc, user.created_at.asc)
        ```
        """
        return self.ordering_class(self, desc=True)


class Symbol:
    """
    Symbols are used as tokens, whose attributes are used to construct arguments
    to various Query instance methods that build a pending "select statment." For example,
    ```
    user = store.symbol()

    get_eligable_users = store.select(      # <- store.select returns a Query object.
        user.name, user.email
    ).where(
        user.created_at > cutoff_date
    )

    eligable_users = get_eligable_users()
    ```
    """

    Attribute = SymbolicAttribute

    def __init__(self):
        self._attrs: Dict[Text, 'SymbolicAttribute'] = {}

    def __getitem__(self, key: Text) -> 'SymbolicAttribute':
        return getattr(self, key)

    def __getattr__(self, key: Text) -> 'SymbolicAttribute':
        """
        This allows the symbol to generate dynamic SymbolicAttributes via
        attribute notation. For example:
        ```
        user = store.symbol()
        attr = user.email
        assert isinstance(attr, SymbolicAttribute)
        assert attr.key == 'email'
        ```

        The SymbolicAttribute is memoized in self._attrs.
        """
        if key not in self._attrs:
            # create and memoize SymbolicAttribute
            attr = SymbolicAttribute(key, symbol=self)
            self._attrs[key] = attr

        return self._attrs[key]

    def __deepcopy__(self, memo) -> 'Symbol':
        copy = type(self)()
        copy._attrs = {
            k: SymbolicAttribute(k, self) for k in self._attrs
        }
        return copy