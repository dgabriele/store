from copy import deepcopy
from typing import Any, Dict, Optional, Text, Set, Union, Iterable

from .interfaces import StateDictInterface, StoreInterface, TransactionInterface


class StateDict(StateDictInterface):
    """
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._store: Optional[StoreInterface] = None
        self._transaction: Optional[TransactionInterface] = None
        self._backend: Optional[Union[TransactionInterface, StoreInterface]] = None

    @property
    def store(self) -> Optional[StoreInterface]:
        return self._store

    @store.setter
    def store(self, store: StoreInterface):
        self._store = store

    @property
    def transaction(self) -> Optional[TransactionInterface]:
        return self._transaction

    @transaction.setter
    def transaction(self, transaction: TransactionInterface):
        self._transaction = transaction

    @property
    def backend(self) -> Optional[Union[TransactionInterface, StoreInterface]]:
        return self._backend

    def __setitem__(self, key: Any, value: Any):
        super().__setitem__(key, value)
        self._backend.update(self, {key})

    def __delitem__(self, key: Any):
        super().__delitem__(key)
        self._backend.delete(self, {key})

    def __deepcopy__(self, memo) -> 'StateDict':
        copy = StateDict(deepcopy(dict(self)))
        copy._store = self._store
        copy._transaction = self._transaction
        return copy

    def update(self, values: Dict, flush=True) -> 'StateDict':
        if flush:
            self._backend.update(self, set(values.keys()))
        super().update(values)
        return self

    def clear(self):
        self._backend.delete(self)
        super().clear()

    def setdefault(self, key: Any, value: Any) -> Any:
        if key not in self:
            self._backend.update(self, key)
        return super().setdefault(key, value)

    def delete(self, keys: Optional[Set[Text]] = None) -> 'StateDict':
        if self.transaction is not None:
            self.transaction.delete(self, keys=keys)
        else:
            self.store.delete(self, keys=keys)
        return self

    def projection(self, keys: Iterable[Text]) -> 'StateDict':
        proj = StateDict({k: v for k, v in self.items() if k in keys})
        proj._store = self._store
        proj._transaction = self._transaction
        return proj