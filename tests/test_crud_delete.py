def test_delete(store_with_data, press_event, click_event):
    store_with_data.delete(press_event)

    assert len(store_with_data.records) == 1
    assert click_event['id'] in store_with_data.records

    store_with_data.delete(click_event)

    assert not store_with_data.records

    indexer = store_with_data.indexer
    for k, index in indexer.indexes.items():
        print(k)
        for record in [press_event, click_event]:
            if k in record:
                v = indexer.make_indexable(record[k])
                assert v not in index