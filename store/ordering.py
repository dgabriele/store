"""
class Ordering
"""

from datetime import datetime, timedelta, date
from store.util import get_hashable
from typing import Iterable, List, Dict, Sequence

from .interfaces import StateDictInterface, OrderingInterface
from .exceptions import NotOrderable


class Ordering(OrderingInterface):
    """
    """

    def __init__(self, attr, desc: bool) -> None:
        super().__init__()
        self.attr = attr
        self.desc = desc

    @staticmethod
    def sort(
        records: Iterable[StateDictInterface],
        orderings: Sequence['Ordering']
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
            datetime: lambda x: x.timestamp(),
            timedelta: lambda x: x.total_seconds(),
            date: lambda x: x.toordinal(),
            dict: get_hashable,
            set: get_hashable,
            tuple: get_hashable,
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
                            raise NotOrderable(key, value)
                else:
                    index.append(value)

            indexes[record] = tuple(index)

        # now that we have indexes dict, do the sort!
        return sorted(records, key=lambda x: indexes[x])