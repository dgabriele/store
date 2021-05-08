from store import Store


store = Store()

store.create({
    'id': 1,
    'name': 'John',
    'location': {'lng': 40.12, 'lat': -20.9},
    'weight': 140,
    'sex': 'M',
})

# get john before transaction
record = store.get(1)
assert record['sex'] == 'M'

# apply sex-change operation within transaction
with store.transaction() as tx:
    record['sex'] = 'F'
    record = tx.update(record)
    assert record['sex'] == 'F'

    # get john's sex in store before commit
    record = store.get(1)
    assert record['sex'] == 'M'

# get john's sex in store after commit
record = store.get(1)
assert record['sex'] == 'F'
