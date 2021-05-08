import bisect

from typing import Any, Text

from .constants import OP_CODE


class Predicate:
    def __init__(self, op: Text) -> None:
        self.op = op

    def __and__(self, other: Any) -> 'BooleanExpression':
        return BooleanExpression(OP_CODE.AND, self, other)

    def __or__(self, other: Any) -> 'BooleanExpression':
        return BooleanExpression(OP_CODE.OR, self, other)

    @classmethod
    def evaluate(cls, store, predicate: 'Predicate'):
        indexer = store.indexer

        if predicate is None:
            return store.records.keys()

        op = predicate.op
        empty = set()
        computed_ids = set()

        if isinstance(predicate, ConditionalExpression):
            k = predicate.attr.key
            v = predicate.value
            index = indexer.indices[k]

            if op == OP_CODE.EQ:
                computed_ids = index.get(v, empty)
            elif op == OP_CODE.NE:
                computed_ids = union([
                    id_set for v_idx, id_set in index.items()
                    if v_idx != v
                ])
            elif op == OP_CODE.IN:
                # containment - we compute the union of all sets of ids whose
                # corresponding records have the given values in the index
                v = v if isinstance(v, set) else set(v)
                computed_ids = union([index.get(k_idx, empty) for k_idx in v])

            elif op == OP_CODE.NOT_IN:
                # the inverse of containment...
                v = v if isinstance(v, set) else set(v)
                computed_ids = union([
                    id_set for v_idx, id_set in index.items()
                    if v_idx not in v
                ])
            else:
                # handle inequalities, computing limit and offset to form an
                # interval with which we index the actual BTree
                keys = list(index.keys())
                offset = None
                interval = None

                if op == OP_CODE.GE:
                    offset = bisect.bisect_left(keys, v)
                    interval = slice(offset, None, 1)

                elif op == OP_CODE.GT:
                    offset = bisect.bisect(keys, v)
                    interval = slice(offset, None, 1)

                elif op == OP_CODE.LT:
                    offset = bisect.bisect_left(keys, v)
                    interval = slice(0, offset, 1)

                elif op == OP_CODE.LEQ:
                    offset = bisect.bisect(keys, v)
                    interval = slice(0, offset, 1)

                assert offset is not None
                assert interval is not None

                computed_ids = union([
                    index[k] for k in keys[interval] if k is not None
                ])
        elif isinstance(predicate, BooleanExpression):
            # recursively compute and union child predicates,
            # left-hand side (lhs) and right-hand side (rhs)
            lhs = predicate.lhs
            rhs = predicate.rhs

            if op == OP_CODE.AND:
                lhs_result = cls.evaluate(store, lhs)
                if lhs_result:
                    rhs_result = cls.evaluate(store, rhs)
                    computed_ids = set.intersection(lhs_result, rhs_result)

            elif op == OP_CODE.OR:
                lhs_result = cls.evaluate(store, lhs)
                rhs_result = cls.evaluate(store, rhs)
                computed_ids = set.union(lhs_result, rhs_result)

        return computed_ids


class ConditionalExpression(Predicate):
    def __init__(self, op: Text, attr: 'SymbolicAttribute', value: Any) -> None:
        super().__init__(op)
        self.attr = attr
        self.key = attr.key
        self.value = value


class BooleanExpression(Predicate):
    def __init__(self, op: Text, lhs: Predicate, rhs: Predicate) -> None:
        super().__init__(op)
        self.lhs = lhs
        self.rhs = rhs