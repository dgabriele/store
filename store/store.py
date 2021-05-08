"""
Base Store class.
"""

from collections import OrderedDict
from threading import RLock
from copy import deepcopy
from typing import (
    Any, Dict, Optional, Set,
    OrderedDict as OrderedDictType,
    Iterable, Text, Union, Callable,
    Type
)

from appyratus.memoize import memoized_property

from .transaction import Transaction
from .symbol import Symbol, SymbolicAttribute
from .query import Query
from .indexer import Indexer
from .util import to_dict
from .dirty import DirtyDict


class Store:
    """
    Store objects act as dict-based in-memory SQL-like databases.
    """

    def __init__(
        self,
        pkey: Text = 'id',
        dict_type: Type[DirtyDict] = DirtyDict,
    ):
        self.pkey_name = pkey
        self.indexer = Indexer(self.pkey_name)
        self.dict_type = dict_type
        self.states = {}
        self.lock = RLock()

    def __contains__(self, pkey: Any) -> bool:
        return pkey in self.states

    def __len__(self) -> int:
        return len(self.states)

    def clear(self):
        self.indexer = Indexer(self.pkey_name)
        self.states = {}

    def make_state_dict(self, data: Dict) -> DirtyDict:
        state = self.dict_type(data)
        state.store = self
        return state

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
        Build a query over the states in the store.
        """
        return Query(self).select(*targets)

    def get(
        self,
        target: Any,
    ) -> Optional[DirtyDict]:
        """
        Return a single state by primary key. If no state exists, return null.
        """
        states = list(self.get_many([target]).values())
        return states[0] if states else None

    def get_many(
        self,
        targets: Iterable[Any],
    ) -> OrderedDictType[Any, DirtyDict]:
        """
        Return a multiple states by primary key. Records are returned in the
        form of a dict, mapping each primary key to a possibly-null state dict.
        Dict keys have the same order as the order with which they are provided
        in the `pkey` primary key argument.
        """
        with self.lock:
            fetched_states = OrderedDict()
            for target in targets:
                if isinstance(target, dict):
                    pkey = target[self.pkey_name]
                else:
                    pkey = getattr(target, self.pkey_name, target)
                state = self.states.get(pkey)
                if state is not None:
                    fetched_states[pkey] = self.make_state_dict(deepcopy(state))
            return fetched_states

    def create(
        self,
        target: Any,
    ) -> DirtyDict:
        """
        Insert a new state in the store.
        """
        state_map = self.create_many([target])
        states = list(state_map.values())
        return states[0]

    def create_many(
        self,
        targets: Iterable[Any],
    ) -> OrderedDictType[Any, DirtyDict]:
        """
        Insert multiple states in the store, returning a mapping of created
        primary key to created state. Dict keys are ordered by insertion.
        """
        created = OrderedDict()

        with self.lock:
            for target in targets:
                # try to convert instance object to dict
                if not isinstance(target, dict):
                    state = to_dict(target)
                else:
                    state = target

                state = deepcopy(state)
                pkey = state[self.pkey_name]

                # store in global primary key map
                self.states[pkey] = state

                # update index B-trees for each 
                if state is not None:
                    self.indexer.insert(state, keys=state.keys())

                # add state to return created dict
                created[pkey] = self.make_state_dict(deepcopy(state))

        return created

    def update(
        self,
        target: Any,
        keys: Optional[Set] = None,
    ) -> DirtyDict:
        """
        Update an existing state in the store, returning the updated state.
        """
        # try to convert instance object to dict
        if not isinstance(target, dict):
            state = to_dict(target)
        else:
            state = target

        pkey = state[self.pkey_name]
        keys = set(keys or state.keys())

        existing_state = self.states[pkey]
        old_state = existing_state.copy()

        with self.lock:
            existing_state.update(state)
            self.indexer.update(old_state, existing_state, keys)
            return self.make_state_dict(deepcopy(state))

    def update_many(
        self,
        targets: Iterable[Any],
    ) -> OrderedDictType[Any, DirtyDict]:
        """
        Update multiple states in the store, returning a mapping from updated
        state primary key to corresponding state. Dict keys preserve the same
        order of the `states` argument.
        """
        updated = OrderedDict()

        with self.lock:
            for target in targets:
                # try to convert instance object to dict
                if not isinstance(target, dict):
                    state = to_dict(target)
                else:
                    state = target

                pkey = state[self.pkey_name]
                existing_state = self.states.get(pkey)

                if existing_state is not None:
                    updated[pkey] = self.update(state)

        return updated

    def delete(
        self,
        target: Any,
        keys: Optional[Iterable[Text]] = None,
    ) -> None:
        """
        Delete an entire state from the store if no `keys` argument supplied;
        otherwise, drop only the specified keys from the stored state.
        """
        if isinstance(target, dict):
            pkey = target[self.pkey_name]
        else:
            pkey = getattr(target, self.pkey_name, target)
        with self.lock:
            if pkey in self.states:
                if not keys:
                    state = self.states.pop(pkey)
                    self.indexer.remove(state)
                else:
                    state = self.states[pkey]
                    for key in keys:
                        if key in state:
                            del state[key]

                    self.indexer.remove(state, keys=keys)

    def delete_many(
        self,
        targets: Iterable[Any],
        keys: Optional[Iterable[Text]] = None,
    ) -> None:
        """
        Delete multiple entire states from the store if no `keys` argument
        supplied; otherwise, drop only the specified keys from the stored
        states.
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
                    state = self.states.get(pkey)
                    if state:
                        for key in keys:
                            if key in state:
                                del state[key]

                        self.indexer.remove(state, keys=keys)