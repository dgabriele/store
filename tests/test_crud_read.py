def test_get(store_with_data, store_state_list):
    # ensure that each inserted state can be retrieved.
    for state in store_state_list:
        fetched_state = store_with_data.get(state['id'])
        assert fetched_state == state


def test_get_many(store_with_data, store_state_list):
    # ensure that each inserted state can be retrieved.
    ids = [state['id'] for state in store_state_list]
    fetched_states = store_with_data.get_many(ids)

    assert isinstance(fetched_states, dict)

    # ensure equal, for both size and order
    assert ids == list(fetched_states.keys())

    # ensure states contain the right data
    for state in store_state_list:
        fetched_state = fetched_states[state['id']]
        assert fetched_state == state