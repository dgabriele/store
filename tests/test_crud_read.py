def test_get(store_with_data, store_state_list):
    # ensure that each inserted record can be retrieved.
    for record in store_state_list:
        fetched_state = store_with_data.get(record['id'])
        assert fetched_state == record


def test_get_many(store_with_data, store_state_list):
    # ensure that each inserted record can be retrieved.
    ids = [record['id'] for record in store_state_list]
    fetched_states = store_with_data.get_many(ids)

    assert isinstance(fetched_states, dict)

    # ensure equal, for both size and order
    assert ids == list(fetched_states.keys())

    # ensure records contain the right data
    for record in store_state_list:
        fetched_state = fetched_states[record['id']]
        assert fetched_state == record