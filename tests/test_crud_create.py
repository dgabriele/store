def test_create(store, press_event):
    state = store.create(press_event)

    # ensure the output dict is identical to the input dict but is not
    # the same object in memory (but rather a copy)
    assert state is not press_event
    assert tuple(state.items()) == tuple(press_event.items())
    
    # check integrity of indexer.keys, which tracks which "column" keys of the
    # state are indexed for a given primary key. Note that the primary key
    # itself is not managed by the indexer.
    pkey = state[store.pkey_name]
    column_keys = set(state.keys())

    assert pkey in store.indexer.keys
    assert column_keys == store.indexer.keys[pkey]

    # ensure that index data structures are indeed lazily constructed
    assert store.pkey_name not in store.indexer.keys
    for k in column_keys:
        assert k in store.indexer.indices
        index = store.indexer.indices[k]
        assert len(index) == 1


def test_create_with_iterable_value_types(store, click_event):
    # ensure (1) that we can insert non-scalar value types, like lists, sets,
    # tuples, dicts, and (2) that they are properly stores and returned.
    state = store.create(click_event)

    assert state['buttons'] == click_event['buttons']
    assert state['position'] == click_event['position']


def test_create_many_returns_expected(store, press_event):
    # note that this test is not very long, as store.create_many internally
    # just calls store.create multiple times -- once for each state.
    states = store.create_many([press_event])
    pkey = press_event[store.pkey_name]

    assert len(states) == 1
    assert set(states.keys()) == {pkey}
    assert states[pkey] == press_event


def test_basic_crud_with_instance_object(store, Person):
    # basid CRUD using a Person instance instead of a dict
    person = Person('abc123', 'Frank', 17, [69, 666, "foo", 1337])

    state = store.create(person)
    assert set(state.keys()) == {
        'id', 'name', 'age', 'lucky_numbers'
    }
    for k, v in state.items():
        assert getattr(person, k) == v

    fetched_state = store.get(person.id)
    for k, v in fetched_state.items():
        assert getattr(person, k) == v

    old_age = person.age
    person.age = 1000
    updated_state = store.update(person, {'age'})
    assert updated_state['age'] != old_age
    for k, v in updated_state.items():
        assert getattr(person, k) == v