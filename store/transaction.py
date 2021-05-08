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

    def create(self, record: Dict) -> Dict:
        self.mutations.append(('create', deepcopy(record), {}))
        return self.front.create(record)
    
    def create_many(self, records: List[Any]) -> OrderedDictType[Any, Dict]:
        self.mutations.append(('create_many', [deepcopy(records)], {}))
        return self.front.create_many(records)

    def select(self, *targets: Union[SymbolicAttribute, Text]) -> Query:
        # TODO: add callback mechanism to Query and use it to merge front
        # records into back
        raise NotImplementedError()
    
    def get(self, pkey: Any) -> Optional[Dict]:
        if pkey not in self.front:
            record = self.back.get(pkey)
            if record is not None:
                self.front.create(record)
        else:
            record = self.front.get(pkey)
        return record

    def get_many(self, pkeys: List[Any]) -> OrderedDictType[Any, Dict]:
        records = self.front.get_many(pkeys)
        missing_pkey_set = set(pkeys) - records.keys()
        if missing_pkey_set:
            back_records = self.back.get_many(pkeys)
            self.front.create_many(back_records.values())
            records.update(back_records)
        return records

    def update(self, target: Any, keys: Optional[Set] = None) -> Dict:
        if not isinstance(target, dict):
            record = to_dict(target)
        else:
            record = target
        self.mutations.append(('update', (deepcopy(record), ), {'keys': keys}))
        pkey = record[self.front.pkey_name]
        if pkey not in self.front:
            back_record = self.back.get(pkey)
            self.front.create(back_record)
            return self.front.update(record, keys=keys)
        return self.front.update(record)

    def update_many(self, targets: List[Dict]) -> OrderedDictType[Any, Dict]:
        records = [
            to_dict(target) if not isinstance(target, dict) else target
            for target in targets
        ]
        self.mutations.append(('update_many', (deepcopy(records), ), {}))
        pkeys = {record[self.front.pkey_name] for record in records}
        missing_pkey_set = pkeys - self.front.records.keys()
        back_records = self.back.get_many(missing_pkey_set)
        self.front.create_many(back_records.values())
        return self.front.update_many(records)

    def delete(self, target: Any, keys: Optional[Iterable[Text]] = None):
        if isinstance(target, dict):
            record = target
        else:
            record = to_dict(target)
        self.mutations.append(('delete', (deepcopy(record), ), {}))
        self.front.delete(record, keys=keys)

    def delete_many(
        self,
        targets: List[Any],
        keys: Optional[Iterable[Text]] = None
    ) -> None:
        records = [
            to_dict(target) if not isinstance(target, dict) else target
            for target in targets
        ]
        self.mutations.append(('delete_many', (deepcopy(records), ), {}))
        self.front.delete_many(records)
