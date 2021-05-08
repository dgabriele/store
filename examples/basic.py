from random import randint
from store import Store


store = Store()
store.create_many([
    {
        'id': randint(1, 100000),
        'name': 'John',
        'location': {'lng': 40.12, 'lat': -20.9},
        'weight': 140,
    },
    {
        'id': randint(1, 100000),
        'name': 'Sarah',
        'location': {'lng': 12.22, 'lat': -31.2},
        'weight': 121,
    },
    {
        'id': randint(1, 100000),
        'name': 'Jeff',
        'location': {'lng': -42.37, 'lat': 11.2},
        'weight': 183,
    },
    {
        'id': randint(1, 100000),
        'name': 'Lydia',
        'location': {'lng': -12.28, 'lat': 33.1},
        'weight': 112,
    },
])

query = store.select(
    store.entry.name,
    store.entry.location
).where(
    store.entry.weight < 130
).order_by(
    store.entry.name.desc
)

ladies = query.execute()
for state in ladies.values():
    print(f'{state["name"]} is located at {state["location"]}.')
