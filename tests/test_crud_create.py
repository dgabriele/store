def test_create(store, press_event):
    record = store.create(press_event)

    # ensure the output dict is identical to the input dict but is not
    # the same object in memory (but rather a copy)
    assert record is not press_event
    assert tuple(record.items()) == tuple(press_event.items())
    
    # check integrity of indexer.keys, which tracks which "column" keys of the
    # record are indexed for a given primary key. Note that the primary key
    # itself is not managed by the indexer.
    primary_key = record[store.pkey_name]
    column_keys = set(record.keys() - {store.pkey_name})

    assert primary_key in store.indexer.keys
    assert column_keys == store.indexer.keys[primary_key]

    # ensure that index data structures are indeed lazily constructed
    assert store.pkey_name not in store.indexer.keys
    for k in column_keys:
        assert k in store.indexer.indexes
        index = store.indexer.indexes[k]
        assert len(index) == 1


def test_create_with_iterable_value_types(store, click_event):
    # ensure (1) that we can insert non-scalar value types, like lists, sets,
    # tuples, dicts, and (2) that they are properly stores and returned.
    record = store.create(click_event)

    assert record['buttons'] == click_event['buttons']
    assert record['position'] == click_event['position']


def test_create_many_returns_expected(store, press_event):
    # note that this test is not very long, as store.create_many internally
    # just calls store.create multiple times -- once for each record.
    records = store.create_many([press_event])
    pkey = press_event[store.pkey_name]

    assert len(records) == 1
    assert set(records.keys()) == {pkey}
    assert records[pkey] == press_event