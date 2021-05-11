# Store
This library provides a `Store` datatype for Python. Each store looks and feels
like an ORM, but unlike an ORM, there is no database on the other end. Instead,
all data lives in memory, in the form of plain Python dicts and B-tree indices.
Stores support **SQL-like _select_ statements** in the style of SQLAlchemy,
**atomic transactions** and **multithreading**.

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

events = Store()

# insert fictitious "event" records
events.create_many([
    {'event_type': 'press', 'char': 'x', 'time': 1},
    {'event_type': 'click', 'button': 'L', 'position': (5, 8), 'time': 2},
    {'event_type': 'click', 'button': 'R', 'position': (3, 4), 'time': 3},
    {'event_type': 'press', 'char': 'y', 'time': 4},
    {'event_type': 'press', 'char': 'p', 'time': 5},
])

with events.transaction() as transaction:
    # delete "click" events after specified time
    transaction.select().where(
        events.row.event_type == 'click',
        events.row.time > 2
    ).delete()

    # capitalize the "char" for each selected "press" event
    get_press_events = transaction.select().where(
        x.event_type == 'press',
        x.char.one_of(['x', 'y', 'z'])
    )
    for event in get_press_events(dtype=list):
        event['char'] = event['char'].upper()
```

## Queries
You can query a store like a SQL database, using _select_, _where_, _order_by_,
_limit_ and _offset_ constraints.

### Symbols
Select statements are written with the help of a class called `Symbol`. A symbol
is a variable used to express what you want to select and how. Suppose you had a
store of "user" records. In this case, you could write query to selects all
users created after a certain cut-off date using a symbol:

```python
user = user_store.symbol()

get_users = user_store.select(
    user.first_name,
    user.email
).where(
    user.created_at > cutoff_date
)

for user in get_users(dtype=list):
    send(message=f'Hello, {user["first_name"]}!', email=user['email'])
```

An alternative to instantiating new symbols for each query is to use a built-in
store attribute, `store.row`. The following query is identical to the one above:

```python
get_users = user_store.select(
    user_store.row.first_name,
    user_store.row.email
).where(
    user_store.row.created_at > cutoff_date
)
```

### Select
By default, an empty select will select everything, like `select * from...` in
SQL; however, if you're only interested in a subset of fields, you can
explicitly enumerate them.

#### Selecting Everything
```python
query = store.select()
```

#### Selecting Specific Fields
```python
query = store.select(store.row.name, store.row.email)
```

### Where
You can constrain queries to select only records whose values match a given
logical predicate. Predicates can be arbitrarily nested in compound boolean
expressions. This is similar to the "where" clause in SQL select statements.

Using a symbol, here are some example:

#### Conditional Expressions
```python
user = store.symbol()

# equality
predicate = (user.email == 'elon.musk@gmail.com')
predicate = (user.email != 'elon.musk@gmail.com')

# inequality
predicate = (user.age >= 50)

# containment
predicate = (user.favorite_color.in(['red', 'blue'])

# logical conjunction (AND)
predicate = (user.scent == 'smelly') & (user.income <= 20000)

# logical disjunction (OR)
predicate = (user.scent == 'smelly') | (user.income <= 20000)

# logical conjunction and disjunction combined
predicate = (
    ((user.scent == 'smelly') | (user.age <= 20)) & (user.name == 'Bob')
)
```

Moreover, predicates can be built up gradually, like so:

```python
predicate = (user.age <= 20)

if some_condition:
    predicate &= (user.income > 100000)   # |= also works
```

Once you have your predicate, you can pass it into a query's `where` method:

```python
query = store.select().where(
    (user.age <= 20) | (user.is_member == True)
)
```

### Order By
Query results can be sorted by one or more values using the `order_by` query
method. For example:

```python
# sort results by age (in ascending order) first
# created_at date (in descending order) second.
query = store.select().order_by(
    user.age.asc,
    user.created_at.desc
)
```

### Limit & Offset
Queries support pagination via limit and offset parameters. The `limit`
parameter is an `int` that determines the maximum number of records returned by
the query while the `offset` parameter determines the starting index of the
returned slice. When using limit and offset, it is important to specify an order, using
`order_by`.

```python
query = store.select(
    user.email
).order_by(
    user.age.desc
).offset(
    20
).limit(
    10
)
```