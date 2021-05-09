from typing import (
    Any, Dict, Iterator, Optional, Set, OrderedDict,
    Iterable, Text, Union, Callable,
)

from .predicate import ConditionalExpression


class StateDictInterface(dict):
    def update(self, values: Dict, flush: bool = True) -> 'StateDictInterface':
        super().update(values)
        return self

    def setdefault(self, key: Any, value: Any) -> Any:
        return super().setdefault(key, value)

    def clear(self):
        raise NotImplementedError()

    def clean(self):
        raise NotImplementedError()

    def save(self, keys: Optional[Set[Text]] = None) -> 'StateDictInterface':
        raise NotImplementedError()

    def delete(self, keys: Optional[Set[Text]] = None) -> 'StateDictInterface':
        raise NotImplementedError()

    def projection(self, keys: Iterable[Text]) -> 'StateDictInterface':
        raise NotImplementedError()


class OrderingInterface:

    attr: 'SymbolicAttributeInterface'
    desc: bool = False


class SymbolicAttributeInterface:
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

    def is_one_of(self, value: Iterable) -> ConditionalExpression:
        raise NotImplementedError()

    @property
    def asc(self) -> OrderingInterface:
        raise NotImplementedError()

    @property
    def desc(self) -> OrderingInterface:
        raise NotImplementedError()


class QueryInterface:
    def execute(self, first=False) -> Optional[Union[StateDictInterface, Dict]]:
        raise NotImplementedError()

    def clear(self):
        raise NotImplementedError()

    def copy(self, store: Optional['StoreInterface'] = None) -> 'QueryInterface':
        raise NotImplementedError()

    def select(self, *targets: Union[Text, SymbolicAttributeInterface]) -> 'QueryInterface':
        raise NotImplementedError()

    def order_by(self, *orderings: Union[Text, SymbolicAttributeInterface, OrderingInterface]) -> 'QueryInterface':
        raise NotImplementedError()

    def where(self, *predicates) -> 'QueryInterface':
        raise NotImplementedError()

    def limit(self, limit: int) -> 'QueryInterface':
        raise NotImplementedError()

    def offset(self, offset: int) -> 'QueryInterface':
        raise NotImplementedError()


class TransactionInterface:

    store: 'StoreInterface'

    def commit(self):
        raise NotImplementedError()

    def rollback(self):
        raise NotImplementedError()

    def create(self, record: Dict) -> Dict:
        raise NotImplementedError()
    
    def create_many(self, records: Iterable[Any]) -> OrderedDict[Any, StateDictInterface]:
        raise NotImplementedError()

    def select(self, *targets: Union[SymbolicAttributeInterface, Text]) -> QueryInterface:
        raise NotImplementedError()
    
    def get(self, targets: Any) -> Optional[StateDictInterface]:
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
    entry: SymbolicAttributeInterface
    records: Dict[Text, Dict]

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
    ) -> StateDictInterface:
        raise NotImplementedError()

    def create_many(
        self,
        targets: Iterable[Any],
    ) -> OrderedDict[Any, StateDictInterface]:
        raise NotImplementedError()

    def update(
        self,
        target: Any,
        keys: Optional[Set] = None,
    ) -> StateDictInterface:
        raise NotImplementedError()

    def update_many(
        self,
        targets: Iterable[Any],
    ) -> OrderedDict[Any, StateDictInterface]:
        raise NotImplementedError()

    def delete(
        self,
        target: Any,
        keys: Optional[Iterable[Text]] = None,
    ) -> None:
        raise NotImplementedError()

    def delete_many(
        self,
        targets: Iterable[Any],
        keys: Optional[Iterable[Text]] = None,
    ) -> None:
        raise NotImplementedError()