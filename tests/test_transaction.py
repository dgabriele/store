from store.transaction import Transaction


def test_create_in_transaction(store, click_event):
    trans = Transaction(store)
    record = trans.create(click_event)
    pkey = click_event['id']

    assert pkey in trans.front.records
    assert pkey not in trans.back.records
    assert len(trans.mutations) == 1
    assert trans.mutations[0][0] == 'create'


def test_create_many_in_transaction(store, click_event, press_event):
    trans = Transaction(store)
    record = trans.create_many([click_event, press_event])

    for event in [click_event, press_event]:
        pkey = click_event['id']
        assert pkey in trans.front.records
        assert pkey not in trans.back.records

    assert len(trans.mutations) == 1
    assert trans.mutations[0][0] == 'create_many'


def test_get_in_transaction(store, click_event):
    trans = Transaction(store)
    trans.create(click_event)
    assert len(trans.mutations) == 1

    record = trans.get(click_event['id'])
    assert record is not None
    assert len(trans.mutations) == 1


def test_get_many_in_transaction(store, click_event, press_event):
    trans = Transaction(store)
    trans.create_many([click_event, press_event])
    assert len(trans.mutations) == 1

    records = trans.get_many([click_event['id'], press_event['id']])
    assert len(records) == 2
    assert len(trans.mutations) == 1


def test_update_in_transaction(store_with_data, click_event):
    old_x = click_event['position']['x']
    new_x = 1213243

    click_event['position']['x'] = new_x

    trans = Transaction(store_with_data)
    trans.update(click_event)

    assert len(trans.mutations) == 1

    record = trans.front.get(click_event['id'])
    assert record is not None
    assert record['position']['x'] == new_x

    record = trans.back.get(click_event['id'])
    assert record['position']['x'] == old_x


def test_update_many_in_transaction(store_with_data, click_event, press_event):
    old_x = click_event['position']['x']
    new_x = 1213243

    old_char = press_event['char']
    new_char = 'z'

    click_event['position']['x'] = new_x
    press_event['char'] = new_char

    trans = Transaction(store_with_data)
    trans.update_many([click_event, press_event])

    assert len(trans.mutations) == 1

    record = trans.front.get(click_event['id'])
    assert record is not None
    assert record['position']['x'] == new_x

    record = trans.front.get(press_event['id'])
    assert record is not None
    assert record['char'] == new_char

    record = trans.back.get(click_event['id'])
    assert record['position']['x'] == old_x

    record = trans.back.get(press_event['id'])
    assert record is not None
    assert record['char'] == old_char


def test_delete_in_transaction(store_with_data, click_event):
    trans = Transaction(store_with_data)
    trans.delete(click_event)
    pkey = click_event['id']
    
    assert len(trans.mutations) == 1
    assert pkey not in trans.front
    assert pkey in trans.back


def test_delete_many_in_transaction(store_with_data, click_event, press_event):
    trans = Transaction(store_with_data)
    trans.delete_many([click_event, press_event])

    assert len(trans.mutations) == 1

    for event in [click_event, press_event]:
        pkey = event['id']
        assert pkey not in trans.front
        assert pkey in trans.back