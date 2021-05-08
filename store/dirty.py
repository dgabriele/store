from copy import deepcopy
from typing import Any, Dict, Optional, Text


class DirtyDict(dict):
    """
    A DirtyDict is a dict that keeps track of which values have been created
    or modified since the last time clear() was called.  The dirty keys are
    kept in self.dirty.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from store.store import Store
        from store.transaction import Transaction

        self.dirty = set(self.keys())
        self.store: Optional[Store] = None
        self.transaction: Optional[Transaction] = None

    def __setitem__(self, key: Any, value: Any):
        super().__setitem__(key, value)
        self.dirty.add(key)

    def __delitem__(self, key: Any):
        super().__delitem__(key)
        self.dirty.discard(key)

    def __deepcopy__(self, memo) -> 'DirtyDict':
        copy = DirtyDict(deepcopy(dict(self)))
        copy.store = self.store
        return copy

    def get_dirty_dict(self) -> Dict:
        return {k: self[k] for k in self.dirty}

    def update(self, values: Dict, clean: bool = False):
        super().update(values)
        if clean:
            self.dirty -= values.keys()
        else:
            self.dirty.update(values.keys())

    def clear(self):
        super().clear()
        self.dirty.clear()

    def setdefault(self, key: Any, value: Any) -> Any:
        if key not in self:
            self.dirty.add(key)
        return super().setdefault(key, value)

    def clean(self):
        self.dirty.clear()

    def save(self) -> 'DirtyDict':
        if self.transaction is not None:
            state = self.transaction.update(self, self.dirty)
        else:
            state = self.store.update(self, self.dirty)
        self.update(state)
        self.clean()
        return self

    def delete(self, keys: Optional[Text] = None) -> 'DirtyDict':
        if self.transaction is not None:
            self.transaction.delete(self, keys=keys)
        else:
            self.store.delete(self, keys=keys)
        return self