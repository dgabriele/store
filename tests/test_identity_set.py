def test_fetched_objects_are_identical(store):
    a = store.create({'name': 'John'})
    b = store.get(a)

    assert a is b


def test_fetched_objects_are_identical_many(store):
    a1 = store.create({'name': 'John'})
    b1 = store.create({'name': 'John'})
    a2, b2 = list(store.get_many([a1, b1]).values())

    assert a1 is a2
    assert b1 is b2


def test_updated_object_is_identical(store):
    a = store.create({'name': 'John'})
    b = a.update({'age': 6})

    assert 'age' in a
    assert a is b


def test_identity_map_is_indeed_weak(store):
    # ensure that the weak ref is indeed reclaimed when no more hard references
    # to the underlying StateDict remain.
    store.create({'name': 'John'})

    assert len(store.identity) == 0