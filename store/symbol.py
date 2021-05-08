import bisect

from functools import reduce
from datetime import datetime, timedelta, date
from typing import Any, List, Optional, OrderedDict, Text, Union, Dict, Iterable

from .util import union
from .exceptions import NotSelectable
from .constants import OP_CODE


class Predicate:
    def __init__(self, op: Text) -> None:
        self.op = op

    def __and__(self, other: Any) -> 'LogicalOperation':
        return LogicalOperation(OP_CODE.AND, self, other)

    def __or__(self, other: Any) -> 'LogicalOperation':
        return LogicalOperation(OP_CODE.OR, self, other)

    @classmethod
    def evaluate(cls, store, predicate: 'Predicate'):
        indexer = store.indexer

        if predicate is None:
            return store.records.keys()

        op = predicate.op
        empty = set()
        computed_ids = set()

        if isinstance(predicate, Comparison):
            k = predicate.attr.key
            v = predicate.value
            index = indexer.indices[k]

            if op == OP_CODE.EQ:
                computed_ids = index.get(v, empty)
            elif op == OP_CODE.NEQ:
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

                if op == OP_CODE.GEQ:
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
        elif isinstance(predicate, LogicalOperation):
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


class Comparison(Predicate):
    def __init__(self, op: Text, attr: 'SymbolicAttribute', value: Any) -> None:
        super().__init__(op)
        self.attr = attr
        self.key = attr.key
        self.value = value


class LogicalOperation(Predicate):
    def __init__(self, op: Text, lhs: Predicate, rhs: Predicate) -> None:
        super().__init__(op)
        self.lhs = lhs
        self.rhs = rhs


class Ordering:
    def __init__(self, attr: 'SymbolicAttribute', desc: bool) -> None:
        self.attr = attr
        self.desc = desc

    @staticmethod
    def sort(
        records: List[Dict],
        orderings: List['Ordering']
    ) -> List['Dict']:
        """
        Perform a multi-key sort on the given record list. This procedure
        approximately O(N log N).
        """

        # if we only have one key to sort by, skip the fancy indexing logic
        # below and just use built-in sorted method as nature intended.
        if len(orderings) == 1:
            key = orderings[0].attr.key
            reverse = orderings[0].desc
            return sorted(records, key=lambda x: x[key], reverse=reverse)

        # create functions for converting types that are not inherently
        # sortable to an integer value (which is sortable)
        converters = {
            bytes: lambda x: int.from_bytes(x, byteorder='big'),
            dict: lambda x: hash(tuple(sorted(x.items()))),
            list: lambda x: hash(tuple(x)),
            set: lambda x: hash(tuple(sorted(x))),
            datetime: lambda x: x.timestamp(),
            timedelta: lambda x: x.total_seconds(),
            date: lambda x: x.toordinal(),
        }

        # pre-compute the "index" keys by which the records shall be sorted.
        # Each index is an array of ints.
        indexes = {}
        for record in records:
            index = []
            for ordering in orderings:
                key = ordering.attr.key
                value = record.get(key)
                if value is None:
                    value = 0
                if ordering.desc:
                    if isinstance(value, str):
                        max_unicode_value = 0x10FFFF
                        inverses = [max_unicode_value - ord(c) for c in value]
                        inverse_value = ''.join(chr(i) for i in inverses)
                        index.append(inverse_value)
                    else:
                        convert = converters.get(type(value))
                        if convert:
                            value = convert(value)
                        index.append(-1 * value)
                else:
                    index.append(value)

            indexes[record] = tuple(index)

        # now that we have indexes dict, do the sort!
        return sorted(records, key=lambda x: indexes[x])


class SymbolicAttribute:
    def __init__(self, key: Text, symbol: Optional['Symbol'] = None) -> None:
        self.symbol = symbol
        self.key = key

    def __lt__(self, value: Any) -> Comparison:
        return Comparison(OP_CODE.LT, self, value)

    def __gt__(self, value: Any) -> Comparison:
        return Comparison(OP_CODE.GT, self, value)

    def __ge__(self, value: Any) -> Comparison:
        return Comparison(OP_CODE.GE, self, value)

    def __le__(self, value: Any) -> Comparison:
        return Comparison(OP_CODE.LE, self, value)

    def __eq__(self, value: Any) -> Comparison:
        return Comparison(OP_CODE.EQ, self, value)

    def __ne__(self, value: Any) -> Comparison:
        return Comparison(OP_CODE.NE, self, value)

    def is_one_of(self, value: Iterable) -> Comparison:
        return Comparison(OP_CODE.IN, self, value)

    @property
    def asc(self) -> Ordering:
        return Ordering(self, desc=False)

    @property
    def desc(self) -> Ordering:
        return Ordering(self, desc=True)


class Symbol:
    def __init__(self):
        self.attrs = {}

    def __getattr__(self, key: Text) -> SymbolicAttribute:
        if key not in self.attrs:
            attr = SymbolicAttribute(key, symbol=self)
            self.attrs[key] = attr
        return self.attrs[key]


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
            records = [
                record.copy() for record in self.store.records.values()
            ]
        else:
            pkeys = Predicate.evaluate(self.store, self.predicate)
            records = list(self.store.get_many(pkeys).values())

        if not records:
            return None if first else {}

        # order the records
        if self.orderings:
            records = Ordering.sort(records, self.orderings)

        # paginate after ordering
        if self.offset_index is not None:
            offset = self.offset_index
            if self.limit_index is not None:
                limit = self.limit_index
                records = records[offset:offset+limit]
            else:
                records = records[offset:]
        elif self.limit_index is not None:
            records = records[:self.limit_index]

        if first:
            if self.selected:
                return {
                    k: v for k, v in records[0].items()
                    if k in self.selected or k == self.store.pkey_name
                }
            else:
                return records[0]

        record_map = OrderedDict()
        for record in records:
            # get projection of only selected keys
            if self.selected:
                record = {
                    k: v for k, v in record.items()
                    if k in self.selected or k == self.store.pkey_name
                }
            record_map[record[self.store.pkey_name]] = record

        return record_map

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