from store.dirty import DirtyDict
from typing import (
    Optional, Text, Any, List,
    OrderedDict as OrderedDictType,
    Dict, Set, Iterable, Union,
    Callable
)
from copy import deepcopy

from .util import to_dict
from .symbol import SymbolicAttribute
from .query import Query


class Transaction:
    def __init__(self, back, front=None, callback: Optional[Callable] = None):
        from store.store import Store

        self.front: Store = front or Store(back.pkey_name)
        self.back: Store = back
        self.mutations = []
        self.callback = callback

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type:
            self.rollback()
            return False
        else:
            self.commit()
            return True

    def commit(self):
        # flush mutations to the backend store,
        # replaying front store calls to the back store.
        for func_name, args, kwargs in self.mutations:
            func = getattr(self.back, func_name)
            func(*args, **kwargs)

        # trigger custom callback method
        if self.callback is not None:
            self.callback(self)

    def rollback(self):
        self.front.clear()
        self.mutations.clear()

    def create(self, state: Dict) -> Dict:
        self.mutations.append(('create', deepcopy(state), {}))
        return self.front.create(state)
    
    def create_many(self, states: List[Any]) -> OrderedDictType[Any, DirtyDict]:
        self.mutations.append(('create_many', [deepcopy(states)], {}))
        return self.front.create_many(states)

    def select(self, *targets: Union[SymbolicAttribute, Text]) -> Query:
        # TODO: add callback mechanism to Query and use it to merge front
        # states into back
        raise NotImplementedError()
    
    def get(self, pkey: Any) -> Optional[DirtyDict]:
        if pkey not in self.front:
            state = self.back.get(pkey)
            if state is not None:
                self.front.create(state)
        else:
            state = self.front.get(pkey)
        state.transaction = self
        return state

    def get_many(self, pkeys: List[Any]) -> OrderedDictType[Any, DirtyDict]:
        states = self.front.get_many(pkeys)
        missing_pkey_set = set(pkeys) - states.keys()
        if missing_pkey_set:
            back_states = self.back.get_many(pkeys)
            self.front.create_many(back_states.values())
            states.update(back_states)
        return states

    def update(self, target: Any, keys: Optional[Set] = None) -> DirtyDict:
        if not isinstance(target, dict):
            state = to_dict(target)
        else:
            state = target
        self.mutations.append(('update', (deepcopy(state), ), {'keys': keys}))
        pkey = state[self.front.pkey_name]
        if pkey not in self.front:
            back_state = self.back.get(pkey)
            self.front.create(back_state)
            return self.front.update(state, keys=keys)
        state = self.front.update(state)
        state.transaction = self
        return state

    def update_many(self, targets: List[Dict]) -> OrderedDictType[Any, DirtyDict]:
        states = [
            to_dict(target) if not isinstance(target, dict) else target
            for target in targets
        ]
        self.mutations.append(('update_many', (deepcopy(states), ), {}))
        pkeys = {
            state[self.front.pkey_name] for state in states
        }
        missing_pkey_set = pkeys - self.front.states.keys()
        back_states = self.back.get_many(missing_pkey_set)
        self.front.create_many(back_states.values())
        states = self.front.update_many(states)
        for state in states.values():
            state.transaction = self
        return states

    def delete(self, target: Any, keys: Optional[Iterable[Text]] = None):
        if isinstance(target, dict):
            state = target
        else:
            state = to_dict(target)
        self.mutations.append(('delete', (deepcopy(state), ), {}))
        self.front.delete(state, keys=keys)

    def delete_many(
        self,
        targets: List[Any],
        keys: Optional[Iterable[Text]] = None
    ) -> None:
        states = [
            to_dict(target) if not isinstance(target, dict) else target
            for target in targets
        ]
        self.mutations.append(
            ('delete_many', (deepcopy(states), ), {'keys': keys})
        )
        self.front.delete_many(states)
