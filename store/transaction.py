from typing import (
    Optional, Text, Any, List,
    OrderedDict as OrderedDictType,
    Dict, Set, Iterable, Union,
    Callable, Tuple
)
from collections import defaultdict

from appyratus.memoize import memoized_property

from .util import get_pkeys, to_dict
from .symbol import Symbol
from .query import Query
from .interfaces import (
    QueryInterface, StoreInterface, TransactionInterface, StateDictInterface
)


class Transaction(TransactionInterface):
    """
    Transactions are created by Stores via the store.transaction() method. In
    code, the Store through which a Transaction was created is called the "back"
    store.

    Operations performed in a transaction are applied to a separate Store
    instance, denoted "front" in code. Upon comitting the transaction, all
    create, update, and delete methods called on the front store are atomically
    applied to the back store.
    """

    def __init__(self, back, front, callback: Optional[Callable] = None):
        super().__init__()
        self.back: StoreInterface = back
        self.front: StoreInterface = front
        self.callback = callback
        self.deleted_pkeys = set()
        self.partially_deleted_pkeys = defaultdict(set)
        self.updated_pkeys = set()
        self.created_pkeys = set()

    def __enter__(self):
        """
        Enter managed context. This is used to implement:

        ```python
        with store.transaction() as trans:
            trans.create(...)
        ````

        The context manager will either commit or rollback the transaction (See:
        the __exit__ method)
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Rollback or commit the transaction at the end of a "with" block.
        """
        if exc_type is not None:
            self.rollback()
            return False
        else:
            self.commit()
            return True

    @memoized_property
    def row(self) -> Symbol:
        """
        For convenience, this can be used when forming queries, like:

        query = store.select(
            store.row.id,
            store.row.email,
            store.row.password,
        ).where(
            store.row.name == 'John'
        )

        """
        return Symbol()

    @property
    def records(self) -> Dict:
        """
        Alias for self.front.records.
        """
        return self.front.records

    def commit(self):
        """
        Atomically replay all journaled store actions (method calls) that were
        applied to self.front to self.back. Afterwards, apply custom "on_commit"
        callback.
        """
        # flush changes to backend store,
        with self.back.lock:
            # flush delete statements
            if self.deleted_pkeys:
                self.back.delete_many(self.deleted_pkeys)

            # flush create statements
            created_pkeys = self.created_pkeys - self.deleted_pkeys
            if created_pkeys:
                self.back.create_many(
                    self.front.records[pkey] for pkey in created_pkeys
                )

            # flush update statements
            updated_pkeys = self.updated_pkeys - self.deleted_pkeys
            if updated_pkeys:
                self.back.update_many(
                    self.front.records[pkey] for pkey in updated_pkeys
                )

            # flush partial deletes
            partial_pkeys = (
                self.partially_deleted_pkeys.keys() - self.deleted_pkeys
            )
            if partial_pkeys:
                for pkey, deleted_keys in self.partially_deleted_pkeys.items():
                    self.back.delete(pkey, keys=deleted_keys)

            # trigger custom callback method
            if self.callback is not None:
                self.callback(self)

            # reinitialize transaction
            self.clear()

    def rollback(self):
        """
        Abort the transaction.
        """
        self.clear()

    def clear(self):
        """
        Clear internal record, reseting the Transaction to its initialized record.
        """
        self.front.clear()
        self.deleted_pkeys.clear()
        self.created_pkeys.clear()
        self.updated_pkeys.clear()

    def create(self, record: Dict) -> Dict:
        """
        Insert a single record dict.
        """
        # create record solely in front store
        return self.front.create(record, transaction=self)
    
    def create_many(
        self, records: Iterable[Any]
    ) -> OrderedDictType[Any, StateDictInterface]:
        """
        Insert multiple record dicts.
        """
        # create records solely in front store
        return self.front.create_many(records, transaction=self)

    def select(self, *targets: Union[Symbol.Attribute, Text]) -> QueryInterface:
        """
        Generate a query.
        """
        def merge(query: Query, back_result: Dict[Any, StateDictInterface]):
            front_query = query.copy(self.front)
            front_result = front_query.execute()

            if isinstance(front_result, dict):
                back_result.update(front_result)
                return back_result
            else:
                if front_result is None:
                    if back_result is not None:
                        self.front.create(back_result)
                        return back_result
                    return None
                else:
                    return back_result

        # we don't want to needless fetch records from the back store, so we
        # will construct a query that excludes any of its records' pkeys.
        front_pkeys = (
            self.deleted_pkeys | self.created_pkeys | self.updated_pkeys
        )
        # create a query for back store
        query = self.back.select(*targets).where(
            self.back.row[self.back.pkey_name].not_in(front_pkeys)
        )
        # call merge upon back query executing
        query.subscribe(merge)

        return query
    
    def get(self, target: Any) -> Optional[StateDictInterface]:
        """
        Get a single record by ID.
        """
        pkey = get_pkeys([target], self.front.pkey_name)[0]

        if pkey in self.deleted_pkeys:
            return None

        # if record not present in front, load from back into font.
        # then return the dict from front.
        if target not in self.front:
            record = self.back.get(pkey)
            if record is not None:
                record = self.front.create(record)
        else:
            record = self.front.get(pkey)

        if record is not None:
            record.transaction = self

        return record

    def get_many(
        self, targets: Iterable[Any]
    ) -> OrderedDictType[Any, StateDictInterface]:
        """
        Get a multiple records by ID.
        """
        pkeys = get_pkeys(targets, self.front.pkey_name, as_set=True)
        pkeys -= self.deleted_pkeys

        # get any records in the font store
        records = self.front.get_many(pkeys)

        # load any records not already in front into front from back
        # and then add these to the returned ID map dict.
        missing_pkey_set = pkeys - records.keys()
        if missing_pkey_set:
            back_states = self.back.get_many(missing_pkey_set)
            self.front.create_many(back_states.values())
            records.update(back_states)

        return records

    def update(
        self, target: Any, keys: Optional[Set] = None
    ) -> StateDictInterface:
        """
        Update a single record.
        """
        if not isinstance(target, dict):
            record = to_dict(target)
        else:
            record = target

        pkey = record[self.front.pkey_name]
        if pkey not in self.front:
            back_state = self.back.get(pkey)
            self.front.create(back_state)

        # only update records in front store
        record = self.front.update(record, keys=keys, transaction=self)
        return record

    def update_many(
        self, targets: Iterable[Dict]
    ) -> OrderedDictType[Any, StateDictInterface]:
        """
        Update multiple records.
        """
        # normalize targets to record dicts
        records = [
            to_dict(target) if not isinstance(target, dict) else target
            for target in targets
        ]

        pkeys = get_pkeys(records, self.front.pkey_name, as_set=True)
        missing_pkey_set = pkeys - self.front.records.keys()

        # get missing records from back and load into front store
        back_states = self.back.get_many(missing_pkey_set)
        self.front.create_many(back_states.values())

        # perform updates to records solely in front store
        records = self.front.update_many(records, transaction=self)
        return records

    def delete(self, target: Any, keys: Optional[Iterable[Text]] = None):
        """
        Drop an entire record or specific keys.
        """
        # delete records only from font store
        self.front.delete(target, keys=keys, transaction=self)

    def delete_many(
        self,
        targets: Iterable[Any],
        keys: Optional[Iterable[Text]] = None
    ) -> None:
        """
        Delete multiple records from the store (or, if keys present, just remove
        the given keys from the target records).
        """
        # delete solely from front store
        self.front.delete_many(targets, transaction=self)
