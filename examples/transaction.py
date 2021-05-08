from store import Store


store = Store()

# insert a new "person" state dict
person = store.create({
    'id': 1,
    'name': 'John',
    'location': {'lng': 40.12, 'lat': -20.9},
    'weight': 140,
    'sex': 'M',
})

# perform "sex change operation" in a transaction
with store.transaction() as transaction:
    person = transaction.get(1)
    person['sex'] = 'F'
    person['weight'] -= 15
    person.save()

    # see that stored state is still unchanged
    person = store.get(1)
    assert person['sex'] == 'M'
    assert person['weight'] == 140

# see that stored state has now changed
person = store.get(person)
assert person['sex'] == 'F'
assert person['weight'] == 125
