import random

from datetime import datetime, timedelta

from appyratus.utils.time_utils import TimeUtils


def test_create_many_speed_test(store):
    count = 10000
    records = [
        {
            'email': 'elon.musk@gmail.com',
            'first_name': 'Elon',
            'last_name': 'Musk',
            'companies': ['SpaceX', 'Tesla', 'SolarCity', 'The Boring Company'],
            'joined_at': datetime.now(),
            'age': 57,
        } for i in range(count)
    ]

    def create():
        return store.create_many(records)

    created, duration = TimeUtils.timed(create)

    print(f'Total time taken: {duration.total_seconds():.3f}')

    assert duration < timedelta(seconds=1.25)
    assert len(created) == count


def test_create_many_speed_test_in_transaction(store):
    count = 10000
    records = [
        {
            'email': 'elon.musk@gmail.com',
            'first_name': 'Elon',
            'last_name': 'Musk',
            'companies': ['SpaceX', 'Tesla', 'SolarCity', 'The Boring Company'],
            'joined_at': datetime.now(),
            'age': 57,
        } for i in range(count)
    ]

    def create():
        with store.transaction() as trans:
            return trans.create_many(records)

    created, duration = TimeUtils.timed(create)

    print(f'Total time taken: {duration.total_seconds():.3f}')

    assert duration < timedelta(seconds=1.5)
    assert len(created) == count


def test_update_many_speed_test(store):
    count = 10000
    records = [
        {
            'email': 'elon.musk@gmail.com',
            'first_name': 'Elon',
            'last_name': 'Musk',
            'companies': ['SpaceX', 'Tesla', 'SolarCity', 'The Boring Company'],
            'joined_at': datetime.now(),
            'age': 57,
        } for i in range(count)
    ]

    store.create_many(records)

    for record in records:
        record['email'] = 'Elon.Musk@gmail.com'
        record['companies'].append('SomethingElse')
        record['joined_at'] += timedelta(days=random.randint(1, 100))

    def update():
        return store.update_many(records)

    updated, duration = TimeUtils.timed(update)

    print(f'Total time taken: {duration.total_seconds():.3f}')

    assert duration < timedelta(seconds=1.25)
    assert len(updated) == count


def test_update_many_speed_test_in_transaction(store):
    count = 10000
    records = [
        {
            'email': 'elon.musk@gmail.com',
            'first_name': 'Elon',
            'last_name': 'Musk',
            'companies': ['SpaceX', 'Tesla', 'SolarCity', 'The Boring Company'],
            'joined_at': datetime.now(),
            'age': 57,
        } for i in range(count)
    ]

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

    assert duration < timedelta(seconds=2.25)
    assert len(updated) == count