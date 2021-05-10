# Store
This library provides a `Store` datatype for Python. Each store looks and feels
like an ORM object yet has no database on the back end. Instead, all data lives
in memory, in the form of plain Python dicts and B-tree indices. Stores support
**SQL-like _select_ statements** in the style of SQLAlchemy, **atomic
transactions** and **multithreading**.

The source code aims to be rebustly documented, as we encourage open-source
collaboration on this Project.

## Use-cases
- Long-running interactive applications, like games.
- Automated trading systems with complex internal state management requirements.
- Stream-processing applications that perform fast _ad hoc_ queries on stream buffers.

## Basic Example
Imagine a system that generates user input events, like _mouse click_ and _key
press_. In this example, we begin a transaction and delete _click events_
created after a specified time and capitalize the pressed character asssociated
with each _key press_ event. These operations are random; however, they
demonstate the basic API.

```python
from store import Store

store = Store()

# insert fictitious "event" records
store.create_many([
    {'event_type': 'press', 'char': 'x', 'time': 1},
    {'event_type': 'click', 'button': 'L', 'position': (5, 8), 'time': 2},
    {'event_type': 'click', 'button': 'R', 'position': (3, 4), 'time': 3},
    {'event_type': 'press', 'char': 'y', 'time': 4},
])

with store.transaction() as trans:
    # delete "click" events after specified time
    trans.select().where(
        store.entry.event_type == 'click',
        store.entry.time > 2
    ).delete()

    # capitalize the "char" in each "press" event
    events = trans.select().where(
        store.entry.event_type == 'press',
        store.entry.char.one_of(['x', 'y', 'z'])
    ).execute()

    for event in events.values():
        event['char'] = event['char'].upper()
```
