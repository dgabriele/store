"""
Classes used to form predicate logic processed by Query.execute().
"""

import bisect

from typing import Any, Set, Text

from .constants import OP_CODE
from .util import union


class Predicate:
    """
    Predicate abstract base class. Predicates can be composed into a tree using
    | and & operators.
    """

    def __init__(self, op_code: Text) -> None:
        self.op_code = op_code

    def __and__(self, other: Any) -> 'BooleanExpression':
        """
        Performs pred1 AND pred2, generating a new predicate. For example:
        ```
        (user.email == 'foo@bar.com) & (user.created_at > cutoff_date)
        ```
        """
        return BooleanExpression(OP_CODE.AND, self, other)

    def __or__(self, other: Any) -> 'BooleanExpression':
        """
        Performs pred1 OR pred2, generating a new predicate. For example:
        ```
        (user.email == 'foo@bar.com) | (user.created_at > cutoff_date)
        ```
        """
        return BooleanExpression(OP_CODE.OR, self, other)

    @classmethod
    def evaluate(cls, store, predicate: 'Predicate') -> Set:
        """
        Evaluate the predicate recursively, producting set of primary keys in
        the given store whose records match the predicate's logic. This is used
        in `Query` execution.
        """
        indexer = store.indexer

        if predicate is None:
            return store.records.keys()

        op_code = predicate.op_code
        empty = set()
        computed_ids = set()

        # is predicate >, <, ==, !=, >=, <=, one_of, etc.?
        if isinstance(predicate, ConditionalExpression):
            # fetch the ID subset according to the predicate
            # and union them into the accumulating computed_ids set.
            key = predicate.attr.key
            val = predicate.value
            index = indexer.indices.get(key)

            # if the index doesn't exist yet, just skip it,
            # as this implies no data with the given key is
            # contained in the store.
            if not index:
                return set()

            if op_code == OP_CODE.EQ:
                computed_ids = index.get(val, empty)
            elif op_code == OP_CODE.NE:
                computed_ids = union([
                    id_set for v_idx, id_set in index.items()
                    if v_idx != val
                ])
            elif op_code == OP_CODE.IN:
                # containment - we compute the union of all sets of ids whose
                # corresponding records have the given values in the index
                val = val if isinstance(val, set) else set(val)
                computed_ids = union([index.get(k_idx, empty) for k_idx in val])

            elif op_code == OP_CODE.NOT_IN:
                # the inverse of containment...
                val = val if isinstance(val, set) else set(val)
                computed_ids = union([
                    id_set for v_idx, id_set in index.items()
                    if v_idx not in val
                ])
            else:
                # handle inequalities, computing limit and offset to form an
                # interval with which we index the actual BTree
                keys = list(index.keys())
                offset = None
                interval = None

                if op_code == OP_CODE.GE:
                    offset = bisect.bisect_left(keys, val)
                    interval = slice(offset, None, 1)

                elif op_code == OP_CODE.GT:
                    offset = bisect.bisect(keys, val)
                    interval = slice(offset, None, 1)

                elif op_code == OP_CODE.LT:
                    offset = bisect.bisect_left(keys, val)
                    interval = slice(0, offset, 1)

                elif op_code == OP_CODE.LEQ:
                    offset = bisect.bisect(keys, val)
                    interval = slice(0, offset, 1)

                assert offset is not None
                assert interval is not None

                computed_ids = union([
                    index[key] for key in keys[interval] if key is not None
                ])
        elif isinstance(predicate, BooleanExpression):
            # recursively compute and union child predicates,
            # left-hand side (lhs) and right-hand side (rhs)
            lhs = predicate.lhs
            rhs = predicate.rhs

            if op_code == OP_CODE.AND:
                lhs_result = cls.evaluate(store, lhs)
                if lhs_result:
                    rhs_result = cls.evaluate(store, rhs)
                    computed_ids = set.intersection(lhs_result, rhs_result)

            elif op_code == OP_CODE.OR:
                lhs_result = cls.evaluate(store, lhs)
                rhs_result = cls.evaluate(store, rhs)
                computed_ids = set.union(lhs_result, rhs_result)

        return computed_ids

    def copy(self) -> 'Predicate':
        raise NotImplementedError()


class ConditionalExpression(Predicate):
    """
    ConditionalExpression represent statements like:
    ```
    user.email == 'foo@bar.baz'
    user.name != 'Bob'
    user.age > 18
    ```
    """
    def __init__(self, op_code: Text, attr, value: Any) -> None:
        super().__init__(op_code)
        self.attr = attr        # <- a SymbolicAttribute object
        self.key = attr.key
        self.value = value

    def copy(self) -> 'ConditionalExpression':
        return type(self)(self.op_code, self.attr, self.value)

class BooleanExpression(Predicate):
    """
    BooleanExpression represent statements like:
    ```
    pred = (user.email == 'foo@bar.baz') & (user.name != 'Bob')
    pred |= (user.age > 18)
    ```
    """
    def __init__(self, op_code: Text, lhs: Predicate, rhs: Predicate) -> None:
        super().__init__(op_code)
        self.lhs = lhs
        self.rhs = rhs

    def copy(self) -> 'BooleanExpression':
        return type(self)(self.op_code, self.lhs.copy(), self.rhs.copy())