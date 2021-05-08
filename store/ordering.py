from datetime import datetime, timedelta, date
from typing import List, Dict



class Ordering:
    def __init__(self, attr: 'SymbolicAttribute', desc: bool) -> None:
        self.attr = attr
        self.desc = desc

    @staticmethod
    def sort(
        states: List[Dict],
        orderings: List['Ordering']
    ) -> List['Dict']:
        """
        Perform a multi-key sort on the given state list. This procedure
        approximately O(N log N).
        """

        # if we only have one key to sort by, skip the fancy indexing logic
        # below and just use built-in sorted method as nature intended.
        if len(orderings) == 1:
            key = orderings[0].attr.key
            reverse = orderings[0].desc
            return sorted(states, key=lambda x: x[key], reverse=reverse)

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

        # pre-compute the "index" keys by which the states shall be sorted.
        # Each index is an array of ints.
        indexes = {}
        for state in states:
            index = []
            for ordering in orderings:
                key = ordering.attr.key
                value = state.get(key)
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

            indexes[state] = tuple(index)

        # now that we have indexes dict, do the sort!
        return sorted(states, key=lambda x: indexes[x])