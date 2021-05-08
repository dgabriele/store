"""
Base Store class.
"""

from collections import OrderedDict
from threading import RLock
from copy import deepcopy
from typing import (
    Any, Dict, Optional, Set,
    OrderedDict as OrderedDictType,
    Iterable, Text, Union, Callable
)

from appyratus.memoize import memoized_property

from .transaction import Transaction
from .symbol import Symbol, SymbolicAttribute, Query
from .indexer import Indexer
from .util import to_dict


class Store:
    """
    Store objects act as dict-based in-memory SQL-like databases.
    """

    def __init__(self, primary_key: Text = 'id'):
        self.pkey_name = primary_key
        self.indexer = Indexer(self.pkey_name)
        self.records = {}
        self.lock = RLock()

    def __contains__(self, pkey: Any) -> bool:
        return pkey in self.records

    def __len__(self) -> int:
        return len(self.records)

    def clear(self):
        self.indexer = Indexer(self.pkey_name)
        self.records = {}

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
        return Transaction(self, callback=callback)

    def select(
        self,
        *targets: Union[SymbolicAttribute, Text],
    ) -> Query:
        """
        Build a query over the records in the store.
        """
        return Query(self).select(*targets)

    def get(
        self,
        pkey: Any,
    ) -> Optional[Dict]:
        """
        Return a single record by primary key. If no record exists, return null.
        """
        records = self.get_many([pkey])
        record = records.get(pkey)
        return record if record else None

    def get_many(
        self,
        pkeys: Iterable[Any],
    ) -> OrderedDictType[Any, Dict]:
        """
        Return a multiple records by primary key. Records are returned in the
        form of a dict, mapping each primary key to a possibly-null record dict.
        Dict keys have the same order as the order with which they are provided
        in the `pkey` primary key argument.
        """
        with self.lock:
            fetched_records = OrderedDict()
            for key in pkeys:
                record = self.records.get(key)
                if record is not None:
                    fetched_records[key] = deepcopy(record)
            return fetched_records

    def create(
        self,
        target: Any,
    ) -> Dict:
        """
        Insert a new record in the store.
        """
        record_map = self.create_many([target])
        records = list(record_map.values())
        return records[0]

    def create_many(
        self,
        targets: Iterable[Any],
    ) -> OrderedDictType[Any, Dict]:
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
                    record = target

                record = deepcopy(record)
                pkey = record[self.pkey_name]

                # store in global primary key map
                self.records[pkey] = record

                # update index B-trees for each 
                if record is not None:
                    self.indexer.insert(record, keys=record.keys())

                # add record to return created dict
                created[pkey] = deepcopy(record)

        return created

    def update(
        self,
        target: Any,
        keys: Optional[Set] = None,
    ) -> Dict:
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
        old_record = existing_record.copy()

        with self.lock:
            existing_record.update(record)
            self.indexer.update(old_record, existing_record, keys)
            return deepcopy(record)

    def update_many(
        self,
        targets: Iterable[Any],
    ) -> OrderedDictType[Any, Dict]:
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
                existing_record = self.records.get(pkey)

                if existing_record is not None:
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
                        pkey = getattr(target, self.pkey_name, pkey)
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