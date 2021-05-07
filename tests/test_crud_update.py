def test_update(store_with_data, press_event):
    # ensure that the returned record after applying an update to a value
    # indeed contains the updated value.
    old_char = press_event['char']
    new_char = 'X'

    fetched_record = store_with_data.get(press_event['id'])
    assert fetched_record['char'] == old_char

    press_event['char'] = new_char
    store_with_data.update(press_event)

    fetched_record = store_with_data.get(press_event['id'])
    assert fetched_record['char'] == new_char


def test_update_for_non_scalar(store_with_data, click_event):
    # ensure that the returned record after applying an update to a value
    # indeed contains the updated value.
    old_value = click_event['position']

    x_old = old_value['x']
    x_new = 453453

    new_value = old_value.copy()
    new_value['x'] = x_new

    fetched_record = store_with_data.get(click_event['id'])
    assert fetched_record['position'] == old_value

    click_event['position'] = new_value
    store_with_data.update(click_event)

    fetched_record = store_with_data.get(click_event['id'])
    assert fetched_record['position'] == new_value

    assert new_value['x'] == new_value['x']
    assert new_value['y'] == old_value['y']