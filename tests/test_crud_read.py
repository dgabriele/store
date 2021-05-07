def test_get(store_with_data, store_record_list):
    # ensure that each inserted record can be retrieved.
    for record in store_record_list:
        fetched_record = store_with_data.get(record['id'])
        assert fetched_record == record


def test_get_many(store_with_data, store_record_list):
    # ensure that each inserted record can be retrieved.
    ids = [record['id'] for record in store_record_list]
    fetched_records = store_with_data.get_many(ids)

    assert isinstance(fetched_records, dict)

    # ensure equal, for both size and order
    assert ids == list(fetched_records.keys())

    # ensure records contain the right data
    for record in store_record_list:
        fetched_record = fetched_records[record['id']]
        assert fetched_record == record