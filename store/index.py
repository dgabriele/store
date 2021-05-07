"""
IndexManager is an internal class, used by Store.
"""

from collections import defaultdict
from typing import Any, Dict, Optional, Iterable, Text

from BTrees.OOBTree import BTree # type: ignore

from .util import is_hashable
from .exceptions import NotHashable


class IndexManager:
    """
    Manages access to B-tree indices for each scalar field of stored records.
    """

    def __init__(self, primary_key: Text):
        self.pkey_name = primary_key
        self.keys = defaultdict(set)  # map from pkey to set of indexed dict keys
        self.indices = {}             # BTree indices

    def insert(self, record: Dict, keys: Iterable[Text]):
        """
        Add the primary key of the given record to the keyed indices.
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
                if key not in self.indices:
                    self.indices[key] = BTree()

                # insert value in index
                index = self.indices[key]
                if value not in index:
                    index[value] = set()
                    index[value].add(pkey)
        except NotHashable as exc:
            raise NotHashable(exc.value, key) from exc

    def remove(self, record: Dict, keys: Optional[Iterable[Text]] = None):
        """
        Remove the primary key of the given record from the keyed indices.
        """
        pkey = record[self.pkey_name]
        keys = keys if isinstance(keys, set) else set(keys or [])
        keys = (keys or self.keys.get(pkey)) - {self.pkey_name}

        # remove keys from key map set and delete entry
        # from keys map if no more keys
        if keys:
            self.keys[pkey] -= keys

            # remove entries in B-tree indices
            key = None
            try:
                for key in keys:
                    # lazy create index
                    if key not in self.indices:
                        self.indices[key] = BTree()
                        continue
                    index = self.indices[key]
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