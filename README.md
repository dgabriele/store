# Store
_Stores_ look and feel like an ORM, but unlike an ORM, there is no
database on the other side. Instead, all data is resides in memory, in the form of plain
Python dicts and B-tree indices. Moreover, Stores support **SQL-like _select_** statements
in the style of _SQLAlchemy_, **transactions**, **multithreading**, and the
source code is rebustly documented.

We greatly encourage open-source collaboration on this Project, so please do
have a look! Thanks and enjoy!

## Possible Use-cases
- Long-running interactive applications, like games.
- Automated trading systems with complicated decision-making logic.
- Stream-processing applications that perform _ad hoc_ queries on a stream.

## Basic Example
Imagine a system that generates user input events, like _mouse click_ and _key
press_. In this example, we delete _click events_ created after specified
timestamp (just an int, in this case) while capitalizing the pressed
character asssociated with each _key press_ event.

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