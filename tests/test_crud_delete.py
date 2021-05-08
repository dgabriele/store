from store.util import get_hashable


def test_delete(store_with_data, press_event, click_event):
    store_with_data.delete(press_event)

    assert len(store_with_data.states) == 1
    assert click_event['id'] in store_with_data.states

    store_with_data.delete(click_event)

    assert not store_with_data.states

    indexer = store_with_data.indexer
    for k, index in indexer.indices.items():
        for state in [press_event, click_event]:
            if k in state:
                v = get_hashable(state[k])
                assert state['id'] not in index[v]