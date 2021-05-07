from collections import OrderedDict, defaultdict
from collections.abc import Hashable
from threading import RLock
from typing import (
    Any, Dict, List, Optional, Type, Set, Tuple,
    OrderedDict as OrderedDictType, Iterable, Text
)

from BTrees.OOBTree import BTree  # type: ignore

from .util import is_hashable
from .exceptions import NotHashable


class Store:
    def __init__(self, primary_key: Text = 'id'):
        self.pkey_name = primary_key
        self.indexer = IndexManager(primary_key)
        self.records = {}
        self.lock = RLock()

    def select(self, *args, **kwargs) -> List[Dict]:
        raise NotImplementedError()

    def get(self, primary_key: Any) -> Optional[Dict]:
        """
        Return a single record by primary key. If no record exists, return null.
        """
        records = self.get_many([primary_key])
        record = records.get(primary_key)
        return record.copy() if record else None

    def get_many(
        self, primary_keys: List[Any]
    ) -> OrderedDictType[Any, Optional[Dict]]:
        """
        Return a multiple records by primary key. Records are returned in the
        form of a dict, mapping each primary key to a possibly-null record dict.
        Dict keys have the same order as the order with which they are provided
        in the `primary_keys` argument.
        """
        with self.lock:
            fetched_records = OrderedDict()
            for key in primary_keys:
                record = self.records.get(key)
                fetched_records[key] = record
            return fetched_records

    def create(self, record: Dict) -> Dict:
        """
        Insert a new record in the store.
        """
        record_map = self.create_many([record])
        records = list(record_map.values())
        return records[0]

    def create_many(self, records: List[Dict]) -> OrderedDictType[Any, Dict]:
        """
        Insert multiple records in the store, returning a mapping of created
        primary key to created record. Dict keys are ordered by insertion.
        """
        created = OrderedDict()

        with self.lock:
            for record in records:
                record = record.copy()
                pkey = record[self.pkey_name]
                # store in global primary key map
                self.records[pkey] = record
                # update index B-trees for each 
                if record is not None:
                    self.indexer.insert(record, keys=record.keys())
                # add record to return created dict
                created[pkey] = record.copy()

        return created

    def update(self, record: Dict, keys: Optional[Set] = None) -> Dict:
        """
        Update an existing record in the store, returning the updated record.
        """
        pkey = record[self.pkey_name]
        keys = set(keys or record.keys())

        existing_record = self.records[pkey]
        old_record = existing_record.copy()

        with self.lock:
            existing_record.update(record)
            self.indexer.update(old_record, existing_record, keys)
            return record.copy()

    def update_many(self, records: List[Dict]) -> OrderedDictType[Any, Dict]:
        """
        Update multiple records in the store, returning a mapping from updated
        record primary key to corresponding record. Dict keys preserve the same
        order of the `records` argument.
        """
        updated = OrderedDict()
        with self.lock:
            for record in records:
                pkey = record[self.pkey_name]
                existing_record = self.records.get(pkey)
                if existing_record is not None:
                    updated[pkey] = self.update(record)
        return updated

    def delete(self, pkey: Any, keys: Optional[Iterable[Text]] = None) -> None:
        """
        Delete an entire record from the store if no `keys` argument supplied;
        otherwise, drop only the specified keys from the stored record.
        """
        if isinstance(pkey, dict):
            pkey = pkey[self.pkey_name]
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
        self, targets: List[Any], keys: Optional[Iterable[Text]] = None
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
                        pkey = target
                    self.delete(pkey)
            else:
                # drop only the keys/columns
                keys = keys if isinstance(keys, set) else set(keys)
                for pkey in pkeys:
                    record = self.records.get(pkey)
                    if record:
                        for key in keys:
                            if key in record:
                                del record[key]

                        self.indexer.remove(record, keys=keys)


class IndexManager:
    """
    Manages access to B-tree indexes for each scalar field of stored records.
    """

    def __init__(self, primary_key: Text):
        self.pkey_name = primary_key
        self.indexes = {}             # BTree indices
        self.keys = defaultdict(set)  # map from pkey to set of indexed dict keys

    def insert(self, record: Dict, keys: Iterable[Text]):
        """
        Add the primary key of the given record to the keyed indexes.
        """
        keys = set(keys) if not isinstance(keys, set) else keys
        keys -= {self.pkey_name}

        pkey = record[self.pkey_name]

        # record dict keys we're inserting in indices
        self.keys[pkey] |= keys

        # insert in indices
        key = None
        try:
            for key in keys:
                value = self.make_indexable(record.get(key))

                # lazy create index
                if key not in self.indexes:
                    self.indexes[key] = BTree()

                # insert value in index
                index = self.indexes[key]
                if value not in index:
                    index[value] = set()
                    index[value].add(pkey)
        except NotHashable as exc:
            raise NotHashable(exc.value, key) from exc

    def remove(self, record: Dict, keys: Optional[Iterable[Text]] = None):
        """
        Remove the primary key of the given record from the keyed indexes.
        """
        pkey = record[self.pkey_name]
        keys = keys if isinstance(keys, set) else set(keys or [])
        keys = (keys or self.keys.get(pkey)) - {self.pkey_name}

        # remove keys from key map set and delete entry
        # from keys map if no more keys
        if keys:
            self.keys[pkey] -= keys

            # remove entries in B-tree indexes
            key = None
            try:
                for key in keys:
                    # lazy create index
                    if key not in self.indexes:
                        self.indexes[key] = BTree()
                        continue
                    index = self.indexes[key]
                    value = self.make_indexable(record[key])
                    index[value].discard(pkey)
                    if not index[value]:
                        del index[value]
            except NotHashable as exc:
                raise NotHashable(exc.value, key) from exc

        # if all keys removed from all indices,
        # remove entry in keys dict
        if not self.keys[pkey]:
            del self.keys[pkey]
        
    def update(self, old_record: Dict, record: Dict, keys: Iterable[Text]):
        """
        Update indices based on how values have changed between old and new
        copies of an updated record.
        """
        keys = keys if isinstance(keys, set) else set(keys) - {self.pkey_name}
        pkey = record[self.pkey_name]

        # keys for which indices need to be updated:
        stale_keys = self.keys[pkey] & keys

        # keys that are not yet indexed:
        new_keys = keys - self.keys[pkey]

        # update stale indices
        if stale_keys:
            self.remove(old_record, stale_keys)
            self.insert(record, stale_keys)

        # insert new indices
        self.insert(record, new_keys)

    def make_indexable(self, value: Any) -> Any:
        """
        Some datatypes, like dicts and sets, are not hashable and can't be
        inserted into index dicts as keys; therefore, we must convert them to a
        form that is. That's what we do here.
        """
        if isinstance(value, dict):
            return tuple(sorted(value.items()))
        elif isinstance(value, set):
            return tuple(sorted(value))
        else:
            if not is_hashable(value):
                raise TypeError(
                    f'cannot store unhashable types, but got {type(value)}'
                )
            return value