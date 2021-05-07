"""
Base Store class.
"""

from collections import OrderedDict
from threading import RLock
from typing import (
    Any, Dict, List, Optional, Set,
    OrderedDict as OrderedDictType,
    Iterable, Text
)

from .index_manager import IndexManager
from .util import to_dict

class Store:
    """
    Store objects act as dict-based in-memory SQL-like databases.
    """

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

    def create(self, record: Any) -> Dict:
        """
        Insert a new record in the store.
        """
        record_map = self.create_many([record])
        records = list(record_map.values())
        return records[0]

    def create_many(self, records: List[Any]) -> OrderedDictType[Any, Dict]:
        """
        Insert multiple records in the store, returning a mapping of created
        primary key to created record. Dict keys are ordered by insertion.
        """
        created = OrderedDict()

        with self.lock:
            for record in records:
                # try to convert instance object to dict
                if not isinstance(record, dict):
                    record = to_dict(record)

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

    def update(self, record: Any, keys: Optional[Set] = None) -> Dict:
        """
        Update an existing record in the store, returning the updated record.
        """
        # try to convert instance object to dict
        if not isinstance(record, dict):
            record = to_dict(record)

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
                # try to convert instance object to dict
                if not isinstance(record, dict):
                    record = to_dict(record)

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