"""
Indexer is an internal class, used by Store.
"""

from collections import defaultdict
from typing import Any, Dict, Optional, Iterable, Text

from BTrees.OOBTree import BTree # type: ignore

from .util import get_hashable
from .exceptions import NotHashable


class Indexer:
    """
    Manages access to B-tree indices for each scalar field of stored states.
    """

    def __init__(self, pkey: Text):
        self.pkey_name = pkey
        self.keys = defaultdict(set)  # map from pkey to set of indexed dict keys
        self.indices = {}             # BTree indices

    def insert(self, state: Dict, keys: Iterable[Text]):
        """
        Add the primary key of the given state to the keyed indices.
        """
        keys = set(keys) if not isinstance(keys, set) else keys

        pkey = state[self.pkey_name]

        # state dict keys we're inserting in indices
        self.keys[pkey] |= keys

        # insert in indices
        key = None
        try:
            for key in keys:
                value = get_hashable(state.get(key))

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

    def remove(self, state: Dict, keys: Optional[Iterable[Text]] = None):
        """
        Remove the primary key of the given state from the keyed indices.
        """
        pkey = state[self.pkey_name]
        keys = keys if isinstance(keys, set) else set(keys or [])
        keys = (keys or self.keys.get(pkey).copy())

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
                        continue
                    index = self.indices[key]
                    value = get_hashable(state[key])
                    index[value].discard(pkey)
                    if not index[value]:
                        del index[value]
                    if not index:
                        del self.indices[key]
            except NotHashable as exc:
                raise NotHashable(exc.value, key) from exc

        # if all keys removed from all indices,
        # remove entry in keys dict
        if not self.keys[pkey]:
            del self.keys[pkey]
        
    def update(self, old_state: Dict, state: Dict, keys: Iterable[Text]):
        """
        Update indices based on how values have changed between old and new
        copies of an updated state.
        """
        keys = keys if isinstance(keys, set) else set(keys)
        pkey = state[self.pkey_name]

        # keys for which indices need to be updated:
        stale_keys = self.keys[pkey] & keys

        # keys that are not yet indexed:
        new_keys = keys - self.keys[pkey]

        # update stale indices
        if stale_keys:
            self.remove(old_state, stale_keys)
            self.insert(state, stale_keys)

        # insert new indices
        self.insert(state, new_keys)