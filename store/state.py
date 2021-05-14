"""
class StateDict
"""

from copy import deepcopy
from typing import Any, Dict, Optional, Text, Set, Union, Iterable

from .interfaces import StateDictInterface, StoreInterface, TransactionInterface


class StateDict(StateDictInterface):
    """
    StateDicts are returned from Store CRUD isntance methods (and also by
    Transaction objects). These dicts are endowed with self-reflective CRUD
    methods, like update, delete, etc. Base dict methods, like __setitem__,
    update, and __delitem__ are overloaded to synchronize the current StateDict
    with its associated store.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.store: Optional[StoreInterface] = None
        self.transaction: Optional[TransactionInterface] = None

    @property
    def backend(self) -> Optional[Union[TransactionInterface, StoreInterface]]:
        """
        Return the Transaction associated with this dict; otherwise, return the
        associated store -- or None.
        """
        return self.transaction or self.store

    def __setitem__(self, key: Any, value: Any):
        """
        Update the item in the Store as well as the dict itself.
        """
        super().__setitem__(key, value)
        self.backend.update(self, {key})

    def __getitem__(self, key: Text) -> Any:
        """
        Get a value. If the key isn't stored in the dict, try to acquire it from
        the store, if possible.
        """
        if key not in self:
            # lazy fetch the value (if it exists in the store)
            pkey = super().__getitem__(self.backend.pkey_name)
            data = self.backend.records.get(pkey)
            if key in data:
                self.update({key: data[key]}, sync=False)

        return super().__getitem__(key)

    def __delitem__(self, key: Any):
        """
        Delete the item from the stored record as well as from the dict itself.
        """
        super().__delitem__(key)
        self.backend.delete(self, {key})

    def __deepcopy__(self, memo) -> 'StateDict':
        """
        This ensures that only the data itself is deep-copied; otherwise, we run
        into all sorts of trouble with unpickle-able references.
        """
        copy = StateDict(deepcopy(dict(self)))
        copy.store = self.store
        copy.transaction = self.transaction
        return copy

    def update(self, values: Dict, sync=True) -> 'StateDict':
        """
        Update values in the dict itself while syncing these changes to the
        associated store at the same time.
        """
        super().update(values)
        if sync:
            # sync to store or transaction
            self.backend.update(self, set(values.keys()))
        return self

    def setdefault(self, key: Any, value: Any) -> Any:
        """
        Set and get a keyed value from the dict, using the default value if
        necessary; at the same time, sync the change to the associated store, if
        need be.
        """
        if key not in self:
            self.backend.update(self, key)

        return super().setdefault(key, value)

    def delete(self, keys: Optional[Set[Text]] = None) -> 'StateDict':
        """
        In the case that no `keys` are provided, delete the entire record from
        the store; otherwise, delete only the named keys from the stored record.
        """
        if self.transaction is not None:
            self.transaction.delete(self, keys=keys)
        else:
            self.store.delete(self, keys=keys)

        return self

    def pop(
        self, key: Text, default: Optional[Any] = None, delete=False
    ) -> Any:
        """
        Remove and return a value from the dict, also deleting it from the
        backend store or transaction.
        """
        if delete:
            if key in self:
                value = super().pop(key)
                self.backend.delete(self, {key})
                return value
            return default
        else:
            return super().pop(key, default)

    def projection(self, keys: Iterable[Text]) -> 'StateDict':
        """
        Return a copy of self, which contains only those keys named in `keys`.
        """
        proj = StateDict({
            k: v for k, v in deepcopy(dict(self)).items() if k in keys
        })
        proj.store = self.store
        proj.transaction = self.transaction

        return proj