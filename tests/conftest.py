from uuid import uuid4
from typing import Dict

import pytest

from store.store import Store


@pytest.fixture(scope='function')
def store() -> Store:
    return Store(primary_key='id')


@pytest.fixture(scope='function')
def store_record_list(press_event, click_event) -> Store:
    return [press_event, click_event]


@pytest.fixture(scope='function')
def store_with_data(store, store_record_list) -> Store:
    for record in store_record_list:
        store.create(record)
    return store


@pytest.fixture(scope='function')
def press_event() -> Dict:
    return {
        'id': uuid4(),
        'type': 'press',
        'char': 'x'
    }


@pytest.fixture(scope='function')
def click_event() -> Dict:
    return {
        'id': uuid4(),
        'type': 'click',
        'buttons': {'L', 'R'},
        'position': {'x': 1, 'y': 2}
    }
