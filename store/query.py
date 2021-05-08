from functools import reduce
from typing import List, Optional, OrderedDict, Text, Union, Dict

from .exceptions import NotSelectable
from .predicate import Predicate
from .symbol import SymbolicAttribute
from .ordering import Ordering


class Query:
    def __init__(self, store) -> None:
        self.store = store
        self.selected: Dict[Text, SymbolicAttribute] = {}
        self.orderings: List[Ordering] = []
        self.predicate: Optional[Predicate] = None
        self.limit_index: Optional[int] = None
        self.offset_index: Optional[int] = None

    def __call__(self, first=False) -> Optional[Dict]:
        return self.execute(first=first)

    def execute(self, first=False) -> Optional[Dict]:
        if self.predicate is None:
            states = [
                state.copy() for state in self.store.states.values()
            ]
        else:
            pkeys = Predicate.evaluate(self.store, self.predicate)
            states = list(self.store.get_many(pkeys).values())

        if not states:
            return None if first else {}

        # order the states
        if self.orderings:
            states = Ordering.sort(states, self.orderings)

        # paginate after ordering
        if self.offset_index is not None:
            offset = self.offset_index
            if self.limit_index is not None:
                limit = self.limit_index
                states = states[offset:offset+limit]
            else:
                states = states[offset:]
        elif self.limit_index is not None:
            states = states[:self.limit_index]

        if first:
            if self.selected:
                return {
                    k: v for k, v in states[0].items()
                    if k in self.selected or k == self.store.pkey_name
                }
            else:
                return states[0]

        state_map = OrderedDict()
        for state in states:
            # get projection of only selected keys
            if self.selected:
                state = {
                    k: v for k, v in state.items()
                    if k in self.selected or k == self.store.pkey_name
                }
            state_map[state[self.store.pkey_name]] = state

        return state_map

    def clear(self):
        self.selected = {}
        self.orderings = []
        self.predicate = None
        self.limit_index = None
        self.offset_index = None

    def select(self, *targets: Union[Text, SymbolicAttribute]) -> 'Query':
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

    def order_by(self, *orderings: Union[Text, SymbolicAttribute, Ordering]) -> 'Query':
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

    def where(self, *predicates: Predicate) -> 'Query':
        if len(predicates) > 1:
            predicate = reduce(lambda x, y: x & y, predicates)
        else:
            predicate = predicates[0]

        if self.predicate is None:
            self.predicate = predicate
        else:
            self.predicate &= predicate

        return self

    def limit(self, limit: int) -> 'Query':
        self.limit_index = max(1, limit)
        return self

    def offset(self, offset: int) -> 'Query':
        self.offset_index = max(0, offset)
        return self