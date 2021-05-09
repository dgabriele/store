from store.util import get_hashable


def test_delete(store, press_event, click_event):
    store.create_many([press_event, click_event])
    assert len(store.records) == 2

    store.delete(press_event)

    assert len(store.records) == 1
    assert click_event['id'] in store.records

    store.delete(click_event)

    assert not store.records

    indexer = store.indexer
    for k, index in indexer.indices.items():
        for record in [press_event, click_event]:
            if k in record:
                v = get_hashable(record[k])
                assert record['id'] not in index[v]