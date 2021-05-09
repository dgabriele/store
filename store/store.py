"""
class Store
"""

from uuid import uuid4
from collections import OrderedDict, defaultdict
from threading import RLock
from copy import deepcopy
from typing import (
    Any, Dict, Optional, Set,
    OrderedDict as OrderedDictType,
    Iterable, Text, Union, Callable,
    Type
)
from weakref import WeakValueDictionary

from appyratus.memoize import memoized_property

from .interfaces import StateDictInterface, StoreInterface
from .transaction import Transaction
from .symbol import Symbol, SymbolicAttribute
from .query import Query
from .indexer import Indexer
from .util import to_dict
from .state import StateDict


class Store(StoreInterface):
    """
    Store objects act as dict-based in-memory SQL-like databases.
    """

    def __init__(
        self,
        pkey: Text = 'id',
        dict_type: Type[StateDict] = StateDict,
    ):
        super().__init__()
        self.pkey_name = pkey
        self.indexer = Indexer(self.pkey_name)
        self.dict_type = dict_type
        self.records: Dict[Text, Dict] = {}
        self.identity = WeakValueDictionary()
        self.lock = RLock()

    def __contains__(self, target: Any) -> bool:
        """
        Does the store contain the given target object. The target object can be
        a primary key value, a dict with a primary key as a dict key, or an
        arbitrary object with a primary key as an attribute.
        """
        if isinstance(target, dict):
            pkey = target[self.pkey_name]
        else:
            pkey = getattr(target, self.pkey_name, target)
        return pkey in self.records

    def __len__(self) -> int:
        """
        Return the number of records in the store.
        """
        return len(self.records)

    def pkey_factory(self, record: Dict) -> Any:
        """
        Return an ID for a record being created.
        """
        pkey = record.get(self.pkey_name)
        if pkey is not None:
            return pkey
        else:
            return uuid4().hex

    def state_dict_factory(self, data: Dict) -> StateDict:
        """
        Create a deep copy of the given data dict, returning a new StateDict.
        """
        record = self.dict_type(deepcopy(data))
        record.store = self
        return record

    def clear(self) -> None:
        """
        Remove all records from the store.
        """
        self.indexer = Indexer(self.pkey_name)
        self.records.clear()

    @staticmethod
    def symbol() -> Symbol:
        """
        Convenience method for instantiating a new Symbol without having to
        import it.
        """
        return Symbol()

    @memoized_property
    def entry(self) -> Symbol:
        """
        For convenience, this can be used when forming queries, like:

        query = store.select(
            store.entry.id,
            store.entry.email,
            store.entry.password,
        ).where(
            store.entry.name == 'John'
        )

        """
        return Symbol()

    def transaction(self, callback: Optional[Callable] = None) -> Transaction:
        """
        Create a new transaction. Can be used in a with-statement, like so:
        
        ```python
        with store.transaction() as trans:
            thing = trans.create(...)
            thing.update({'foo': 'bar', 'spam': 'eggs'})
            other_thing = trans.get(...)
            other_thing.delete()
        """
        front = type(self)(self.pkey_name, dict_type=self.dict_type)
        return Transaction(self, front, callback=callback)

    def select(self, *targets: Union[SymbolicAttribute, Text]) -> Query:
        """
        Build a query over the records in the store.
        """
        return Query(self).select(*targets)

    def get(self, target: Any) -> Optional[StateDictInterface]:
        """
        Return a single record by primary key. If no record exists, return null.
        """
        records = list(self.get_many([target]).values())
        return records[0] if records else None

    def get_many(
        self,
        targets: Optional[Iterable[Any]] = None,
    ) -> OrderedDictType[Any, StateDictInterface]:
        """
        Return a multiple records by primary key. Records are returned in the
        form of a dict, mapping each primary key to a possibly-null record dict.
        Dict keys have the same order as the order with which they are provided
        in the `pkey` primary key argument.
        """
        with self.lock:
            if not targets:
                # return all records
                state_dicts = OrderedDict()
                for pkey in self.records.keys() - self.identity.keys():
                    record = self.records[pkey]
                    state_dict = self.state_dict_factory(record)
                    state_dicts[pkey] = state_dict
                return state_dicts
            else:
                fetched_states = OrderedDict()
                for target in targets:
                    if isinstance(target, dict):
                        pkey = target[self.pkey_name]
                    else:
                        pkey = getattr(target, self.pkey_name, target)
                    record = self.records.get(pkey)
                    if record is not None:
                        state_dict = self.identity.get(pkey)
                        if state_dict is not None:
                            state_dict.update(record, sync=False)
                        else:
                            state_dict = self.state_dict_factory(record)
                            self.identity[pkey] = state_dict

                        fetched_states[pkey] = state_dict

                return fetched_states

    def create(self, target: Any) -> StateDictInterface:
        """
        Insert a new record in the store.
        """
        state_map = self.create_many([target])
        records = list(state_map.values())
        return records[0]

    def create_many(
        self,
        targets: Iterable[Any],
    ) -> OrderedDictType[Any, StateDict]:
        """
        Insert multiple records in the store, returning a mapping of created
        primary key to created record. Dict keys are ordered by insertion.
        """
        created = OrderedDict()

        with self.lock:
            for target in targets:
                # try to convert instance object to dict
                if not isinstance(target, dict):
                    record = to_dict(target)
                else:
                    if isinstance(target, StateDict):
                        record = dict(target)
                    else:
                        record = target

                record[self.pkey_name] = self.pkey_factory(record)
                record = deepcopy(record)
                pkey = record[self.pkey_name]

                # store in global primary key map
                self.records[pkey] = record
                state_dict = self.state_dict_factory(record)
                self.identity[pkey] = state_dict

                # update index B-trees for each 
                if record is not None:
                    self.indexer.insert(record, keys=record.keys())

                # add record to return created dict
                created[pkey] = state_dict

        return created

    def update(
        self,
        target: Any,
        keys: Optional[Set] = None,
    ) -> StateDictInterface:
        """
        Update an existing record in the store, returning the updated record.
        """
        # try to convert instance object to dict
        if not isinstance(target, dict):
            record = to_dict(target)
        else:
            record = target

        pkey = record[self.pkey_name]
        keys = set(keys or record.keys())

        existing_record = self.records[pkey]
        old_state = existing_record.copy()

        with self.lock:
            if not keys:
                existing_record.update(record)
            else:
                existing_record.update({
                    k: v for k, v in record.items() if k in record
                })

            self.indexer.update(old_state, existing_record, keys)

            state_dict = self.identity.get(pkey)
            if state_dict:
                state_dict.update(record, sync=False)
            else:
                state_dict = self.state_dict_factory(existing_record)
                self.identity[pkey] = state_dict

            return state_dict

    def update_many(
        self,
        targets: Iterable[Any],
    ) -> OrderedDictType[Any, StateDict]:
        """
        Update multiple records in the store, returning a mapping from updated
        record primary key to corresponding record. Dict keys preserve the same
        order of the `records` argument.
        """
        updated = OrderedDict()

        with self.lock:
            for target in targets:
                # try to convert instance object to dict
                if not isinstance(target, dict):
                    record = to_dict(target)
                else:
                    record = target

                pkey = record[self.pkey_name]
                existing_state = self.records.get(pkey)

                if existing_state is not None:
                    updated[pkey] = self.update(record)

        return updated

    def delete(
        self,
        target: Any,
        keys: Optional[Iterable[Text]] = None,
    ) -> None:
        """
        Delete an entire record from the store if no `keys` argument supplied;
        otherwise, drop only the specified keys from the stored record.
        """
        if isinstance(target, dict):
            pkey = target[self.pkey_name]
        else:
            pkey = getattr(target, self.pkey_name, target)
        with self.lock:
            if pkey in self.records:
                if not keys:
                    record = self.records.pop(pkey)
                    self.indexer.remove(record)
                else:
                    record = self.records[pkey]
                    for key in keys:
                        if key in record:
                            del record[key]

                    self.indexer.remove(record, keys=keys)

    def delete_many(
        self,
        targets: Iterable[Any],
        keys: Optional[Iterable[Text]] = None,
    ) -> None:
        """
        Delete multiple entire records from the store if no `keys` argument
        supplied; otherwise, drop only the specified keys from the stored
        records.
        """
        with self.lock:
            if not keys:
                # drop entire objects
                for target in targets:
                    if isinstance(target, dict):
                        pkey = target[self.pkey_name]
                    else:
                        pkey = getattr(target, self.pkey_name, target)
                    self.delete(pkey)
            else:
                # drop only the keys/columns
                keys = keys if isinstance(keys, set) else set(keys)
                for target in targets:
                    if isinstance(target, dict):
                        pkey = target[self.pkey_name]
                    else:
                        pkey = target
                    record = self.records.get(pkey)
                    if record:
                        for key in keys:
                            if key in record:
                                del record[key]

                        self.indexer.remove(record, keys=keys)