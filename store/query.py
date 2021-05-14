"""
Query main class.
"""

from copy import deepcopy
from functools import reduce
from collections import OrderedDict
from typing import (
    Any, Callable, Iterable, List, Optional,
    Set, Text, Type, Union, Dict
)

from .interfaces import StateDictInterface, StoreInterface, QueryInterface
from .exceptions import NotSelectable
from .predicate import Predicate
from .symbol import SymbolicAttribute
from .ordering import Ordering


class Query(QueryInterface):
    """
    Query objects are returned from Store.select and Transaction.select. It
    represents a SQL-like expression, including select, where, order_by, limit
    and offset components. For example,

    ```python
    query = store.select(
        store.row.type,
        store.row.species,
        store.row.common_name,
    ).where(
        store.row.type == 'fruit',
        store.row.discovery_date > (now - timedelta(years=50)
    ).order_by(
        store.row.discovery_date.asc
    ).limit(
        10
    )

    fruits = query.execute()  # or simply: query()
    ```
    """

    def __init__(self, store: StoreInterface) -> None:
        """
        - `predicate`: the where predicate
        - `orderings`: list of Ordering objects, like [user.name.asc, ...]
        - `selected`: dict of selected keys, like {'name': SymbolicAttribute(name)}
        - `limit_index`: the limit int
        - `offset_index`: the offset int
        - `callbacks`: callback functions executed after query.execute()
        """
        super().__init__()
        self.store = store
        self.selected: Dict[Text, SymbolicAttribute] = {}
        self.orderings: List[Ordering] = []
        self.predicate: Optional[Predicate] = None
        self.limit_index: Optional[int] = None
        self.offset_index: Optional[int] = None
        self.callbacks: Set[Callable] = set()

    def __call__(self, *args, **kwargs) -> Any:
        """
        Execute the query. This is a passthru for self.execute.
        """
        return self.execute(*args, **kwargs)

    def execute(
        self, first=False, dtype: Type = OrderedDict
    ) -> Optional[Union[StateDictInterface, Dict, Iterable]]:
        """
        Execute the query, returning either a single StateDict or an ID map of
        multiple.
        """
        def project(record, selected, pkey_name) -> StateDictInterface:
            """Return a projection of the given record dict"""
            if selected:
                keys = set(selected.keys()) | {pkey_name}
                return record.projection(keys)
            else:
                return record

        def execute_callbacks(query, result):
            """Execute callbacks assigned to the query"""
            for func in self.callbacks:
                func(query, result)

        # if there's no "where" clause to the query, interpret it as
        # a query selecting everything....
        if self.predicate is None:
            records = list(self.store.get_many().values())

        # otherwise, evaluate the where-Predicate, returning a set of IDs
        # to then select via get_many.
        else:
            pkeys = Predicate.evaluate(self.store, self.predicate)
            records = list(self.store.get_many(pkeys).values())

        # if no records return, just return
        if not records:
            return None if first else OrderedDict()

        # order the records
        if self.orderings:
            records = Ordering.sort(records, self.orderings)

        # paginate after ordering
        if self.offset_index is not None:
            offset = self.offset_index
            if self.limit_index is not None:
                limit = self.limit_index
                records = records[offset:offset+limit]
            else:
                records = records[offset:]
        elif self.limit_index is not None:
            records = records[:self.limit_index]

        pkey_name = self.store.pkey_name
        retval = None

        # compute the return value based on dtype
        if first:
            # only return the first record dict
            record = project(records[0], self.selected, pkey_name)
            execute_callbacks(self, record)
            retval = record
        elif issubclass(dtype, dict):
            retval = dtype() # ID => StateDict
            for record in records:
                pkey = record[self.store.pkey_name]
                retval[pkey] = project(record, self.selected, pkey_name)
        else:
            retval = dtype(
                project(record, self.selected, pkey_name)
                for record in records
            )
            retval.index = retval[self.store.pkey_name]

        # pass return value into execution callbaks
        execute_callbacks(self, retval)

        return retval

    def clear(self) -> None:
        """
        Clear all internal state. This returns the query to its newly
        initialized state.
        """
        self.selected = {}
        self.orderings = []
        self.predicate = None
        self.limit_index = None
        self.offset_index = None
        self.callbacks.clear()

    def subscribe(self, callback: Callable) -> None:
        """
        Add a callback function to call after the query executes. The callback
        takes two arguments: the Query object and the return value from
        execute() (a single StateDict if `first` else an ID map).
        """
        self.callbacks.add(callback)

    def unsubscribe(self, callback: Callable) -> None:
        """
        The inverse of `self.subscribe`.
        """
        self.callbacks.discard(callback)

    def delete(
        self, keys: Optional[Iterable[Text]] = None
    ) -> Dict[Text, StateDictInterface]:
        """
        Execute the query and delete the results. Return the results after.
        """
        records = self.execute()
        assert isinstance(records, dict)

        self.store.delete_many(records.values(), keys=keys)
        return records

    def copy(self, store: Optional[StoreInterface] = None) -> 'Query':
        """
        Create a copy of this Query, optionally with a different store instance
        than this query's self.store.
        """
        query = Query(store=store or self.store)
        query.selected = {k: v.copy() for k, v in self.selected.items()}
        query.predicate = self.predicate.copy()
        query.orderings = deepcopy(self.orderings)
        query.limit_index = self.limit_index
        query.offset_index = self.offset_index
        return query

    def select(
        self,
        *targets: Union[Text, SymbolicAttribute],
        append: bool = True
    ) -> 'Query':
        """
        Add or replace additional keys to select from the store, like
        query.select(user.name, user.email, 'age').
        """
        if not append:
            self.selected.clear()

        for target in targets:
            if isinstance(target, str):
                key = target
                if key not in self.selected:
                    attr = SymbolicAttribute(key)
                    self.selected[key] = attr
            elif isinstance(target, SymbolicAttribute):
                attr = target
                if attr.key not in self.selected:
                    self.selected[attr.key] = attr
            else:
                raise NotSelectable(target)
        return self

    def where(
        self,
        *predicates: Predicate,
        append: bool = True
    ) -> 'Query':
        """
        Add "where" predicates, like `query.where(user.name == 'John')`. If
        multiple predicates are supplied, they are composed with a logical
        conjunction (AND'ed together).
        """
        if not append:
            self.predicate = None

        if len(predicates) > 1:
            predicate = reduce(lambda x, y: x & y, predicates)
        else:
            predicate = predicates[0]

        if self.predicate is None:
            self.predicate = predicate
        else:
            self.predicate &= predicate

        return self

    def order_by(
        self,
        *orderings: Union[Text, SymbolicAttribute, Ordering],
        append: bool = True
    ) -> 'Query':
        """
        Add or replace order-by constraints. For example, you could do:
        `query.order_by(user.name.asc, user.age.desc)`.
        """
        if not append:
            self.orderings.clear()

        for obj in orderings:
            if isinstance(obj, str):
                key = obj
                ordering = Ordering(SymbolicAttribute(key), desc=False)
                self.orderings.append(ordering)
            elif isinstance(obj, SymbolicAttribute):
                attr= obj
                ordering = Ordering(attr, desc=False)
                self.orderings.append(ordering)
            else:
                ordering = obj
                assert isinstance(ordering, Ordering)
                self.orderings.append(ordering)

        return self

    def limit(self, limit: int) -> 'Query':
        """
        Set the pagination "limit", as in the maximum number of records to
        return per "page."
        """
        self.limit_index = max(1, limit)
        return self

    def offset(self, offset: int) -> 'Query':
        """
        Set the initial "page" offset. For example, if the query matches 100
        records with an offset of 10, then, `query.execute()` would return
        `records[10:]`.
        """
        self.offset_index = max(0, offset)
        return self