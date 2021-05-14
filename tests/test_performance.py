import random

from string import ascii_letters
from datetime import datetime, timedelta
from typing import Text

from appyratus.utils.time_utils import TimeUtils


def randstr(length: int) -> Text:
    return ''.join(random.choice(ascii_letters) for _ in range(length))


def randdicts(count):
    return [
        {
            'email': randstr(20),
            'first_name': randstr(8),
            'last_name': randstr(8),
            'companies': [randstr(random.randint(5, 10)) for _ in range(5)],
            'joined_at': datetime.now(),
            'age': random.randint(13, 100),
        } for i in range(count)
    ]


def test_create_many_speed(store):
    count = 10000
    records = randdicts(count)

    def create():
        return store.create_many(records)

    created, duration = TimeUtils.timed(create)

    print(f'Total time taken: {duration.total_seconds():.3f}')

    assert duration < timedelta(seconds=2)
    assert len(created) == count


def test_create_many_speed_in_transaction(store):
    count = 10000
    records = randdicts(count)

    def create():
        with store.transaction() as trans:
            return trans.create_many(records)

    created, duration = TimeUtils.timed(create)

    print(f'Total time taken: {duration.total_seconds():.3f}')

    assert duration < timedelta(seconds=3)
    assert len(created) == count


def test_update_many_speed(store):
    count = 10000
    records = randdicts(count)

    store.create_many(records)

    for record in records:
        record['email'] = 'Elon.Musk@gmail.com'
        record['companies'].append('SomethingElse')
        record['joined_at'] += timedelta(days=random.randint(1, 100))

    def update():
        return store.update_many(records)

    updated, duration = TimeUtils.timed(update)

    print(f'Total time taken: {duration.total_seconds():.3f}')

    assert duration < timedelta(seconds=2)
    assert len(updated) == count


def test_update_many_speed_in_transaction(store):
    count = 10000
    records = randdicts(count)

    store.create_many(records)

    for record in records:
        record['email'] = 'Elon.Musk@gmail.com'
        record['companies'].append('SomethingElse')
        record['joined_at'] += timedelta(days=random.randint(1, 100))

    def update():
        with store.transaction() as trans:
            return trans.update_many(records)

    updated, duration = TimeUtils.timed(update)

    print(f'Total time taken: {duration.total_seconds():.3f}')

    assert duration < timedelta(seconds=3.0)
    assert len(updated) == count


def test_query_speed(store):
    count = 10000
    records = randdicts(count)

    store.create_many(records)

    def timed_func():
        with store.transaction() as trans:
            return trans.select().where(
                trans.row.email < 'f',
                trans.row.first_name >= 'x'
            ).order_by(
                trans.row.email.desc
            ).execute()

    records, duration = TimeUtils.timed(timed_func)

    print(f'Total time taken: {duration.total_seconds():.3f}')

    assert duration < timedelta(seconds=0.5)
    assert len(records) > 0
    assert len(records) < count

    for state in records.values():
        assert state['email'][0] < 'f'
        assert state['first_name'][0] >= 'x'
