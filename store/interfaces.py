"""
Public Interface classes for internal components
"""

from threading import RLock
from typing import (
    Any, Dict, Optional, Set, OrderedDict,
    Iterable, Text, Type, Union, Callable,
)

from .predicate import ConditionalExpression


class StateDictInterface(dict):

    store: Optional['StoreInterface'] = None
    transaction: Optional['TransactionInterface'] = None

    def update(self, values: Dict, **kwargs) -> 'StateDictInterface':
        super().update(values)
        return self

    def delete(self, keys: Optional[Set[Text]] = None) -> 'StateDictInterface':
        raise NotImplementedError()

    def projection(self, keys: Iterable[Text]) -> 'StateDictInterface':
        raise NotImplementedError()


class OrderingInterface:

    attr: 'SymbolicAttributeInterface'
    desc: bool = False

    def __init__(self, *args, **kwargs) -> None:
        pass


class SymbolicAttributeInterface:

    key: Text

    def __init__(self, *args, **kwargs) -> None:
        pass

    def __lt__(self, value: Any) -> ConditionalExpression:
        raise NotImplementedError()

    def __gt__(self, value: Any) -> ConditionalExpression:
        raise NotImplementedError()

    def __ge__(self, value: Any) -> ConditionalExpression:
        raise NotImplementedError()

    def __le__(self, value: Any) -> ConditionalExpression:
        raise NotImplementedError()

    def __eq__(self, value: Any) -> ConditionalExpression:
        raise NotImplementedError()

    def __ne__(self, value: Any) -> ConditionalExpression:
        raise NotImplementedError()

    def one_of(self, value: Iterable) -> ConditionalExpression:
        raise NotImplementedError()

    @property
    def asc(self) -> OrderingInterface:
        raise NotImplementedError()

    @property
    def desc(self) -> OrderingInterface:
        raise NotImplementedError()


class QueryInterface:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def subscribe(self, callback: Callable) -> None:
        raise NotImplementedError()

    def unsubscribe(self, callback: Callable) -> None:
        raise NotImplementedError()

    def execute(
        self, first=False, dtype: Type = dict,
    ) -> Optional[Union[StateDictInterface, Dict]]:
        raise NotImplementedError()

    def clear(self):
        raise NotImplementedError()

    def copy(self, store: Optional['StoreInterface'] = None) -> 'QueryInterface':
        raise NotImplementedError()

    def select(
        self,
        *targets: Union[Text, SymbolicAttributeInterface],
        append: bool = True,
    ) -> 'QueryInterface':
        raise NotImplementedError()

    def order_by(
        self,
        *orderings: Union[Text, SymbolicAttributeInterface, OrderingInterface],
        append: bool = True
    ) -> 'QueryInterface':
        raise NotImplementedError()

    def where(
        self,
        *predicates,
        append: bool = True
    ) -> 'QueryInterface':
        raise NotImplementedError()

    def limit(self, limit: int) -> 'QueryInterface':
        raise NotImplementedError()

    def offset(self, offset: int) -> 'QueryInterface':
        raise NotImplementedError()


class TransactionInterface:

    store: 'StoreInterface'

    def __init__(self, *args, **kwargs) -> None:
        pass

    def commit(self):
        raise NotImplementedError()

    def rollback(self):
        raise NotImplementedError()

    def create(self, record: Dict) -> StateDictInterface:
        raise NotImplementedError()
    
    def create_many(self, records: Iterable[Any]) -> OrderedDict[Any, StateDictInterface]:
        raise NotImplementedError()

    def select(self, *targets: Union[SymbolicAttributeInterface, Text]) -> QueryInterface:
        raise NotImplementedError()
    
    def get(self, target: Any) -> Optional[StateDictInterface]:
        raise NotImplementedError()

    def get_many(self, targets: Iterable[Any]) -> OrderedDict[Any, StateDictInterface]:
        raise NotImplementedError()

    def update(self, target: Any, keys: Optional[Set] = None) -> StateDictInterface:
        raise NotImplementedError()

    def update_many(
        self, targets: Iterable[Dict]) -> OrderedDict[Any, StateDictInterface]:
        raise NotImplementedError()

    def delete(
        self, target: Any, keys: Optional[Iterable[Text]] = None
    ) -> None:
        raise NotImplementedError()

    def delete_many(
        self, targets: Iterable[Any], keys: Optional[Iterable[Text]] = None
    ) -> None:
        raise NotImplementedError()


class StoreInterface:

    pkey_name: Text
    row: SymbolicAttributeInterface
    records: Dict[Text, Dict]
    lock: RLock
    
    def __init__(self, *args, **kwargs) -> None:
        pass

    @staticmethod
    def symbol() -> SymbolicAttributeInterface:
        raise NotImplementedError()

    def clear(self):
        raise NotImplementedError()

    def transaction(
        self, callback: Optional[Callable] = None
    ) -> TransactionInterface:
        raise NotImplementedError()

    def select(
        self,
        *targets: Union[SymbolicAttributeInterface, Text],
    ) -> QueryInterface:
        raise NotImplementedError()

    def get(
        self,
        target: Any,
    ) -> Optional[StateDictInterface]:
        raise NotImplementedError()

    def get_many(
        self,
        targets: Optional[Iterable[Any]] = None,
    ) -> OrderedDict[Any, StateDictInterface]:
        raise NotImplementedError()

    def create(
        self,
        target: Any,
        transaction: Optional[TransactionInterface] = None
    ) -> StateDictInterface:
        raise NotImplementedError()

    def create_many(
        self,
        targets: Iterable[Any],
        transaction: Optional[TransactionInterface] = None
    ) -> OrderedDict[Any, StateDictInterface]:
        raise NotImplementedError()

    def update(
        self,
        target: Any,
        keys: Optional[Set] = None,
        transaction: Optional[TransactionInterface] = None
    ) -> StateDictInterface:
        raise NotImplementedError()

    def update_many(
        self,
        targets: Iterable[Any],
        transaction: Optional[TransactionInterface] = None
    ) -> OrderedDict[Any, StateDictInterface]:
        raise NotImplementedError()

    def delete(
        self,
        target: Any,
        keys: Optional[Iterable[Text]] = None,
        transaction: Optional[TransactionInterface] = None
    ) -> None:
        raise NotImplementedError()

    def delete_many(
        self,
        targets: Iterable[Any],
        keys: Optional[Iterable[Text]] = None,
        transaction: Optional[TransactionInterface] = None
    ) -> None:
        raise NotImplementedError()