from store.constants import OP_CODE
from store.symbol import Symbol, SymbolicAttribute
from store.query import Query
from store.predicate import ConditionalExpression, BooleanExpression
from store.ordering import Ordering


def test_symbol_adds_attributes():
    user = Symbol()
    user.name

    assert 'name' in user.attrs
    assert isinstance(user.attrs['name'], SymbolicAttribute)
    assert user.name.symbol is user


def test_comparison_predicates_created():
    user = Symbol()
    cmp1 = user.thing == 1
    cmp2 = user.thing != 1
    cmp3 = user.thing > 1
    cmp4 = user.thing < 1
    cmp5 = user.thing >= 1
    cmp6 = user.thing <= 1

    assert isinstance(cmp1, ConditionalExpression)

    assert cmp1.op_code == OP_CODE.EQ
    assert cmp1.key == 'thing'
    assert cmp1.value == 1

    assert cmp2.op_code == OP_CODE.NE
    assert cmp2.key == 'thing'
    assert cmp2.value == 1

    assert cmp3.op_code == OP_CODE.GT
    assert cmp3.key == 'thing'
    assert cmp3.value == 1

    assert cmp4.op_code == OP_CODE.LT
    assert cmp4.key == 'thing'
    assert cmp4.value == 1

    assert cmp5.op_code == OP_CODE.GE
    assert cmp5.key == 'thing'
    assert cmp5.value == 1

    assert cmp6.op_code == OP_CODE.LE
    assert cmp6.key == 'thing'
    assert cmp6.value == 1


def test_logical_operation_created():
    user = Symbol()

    p1 = (user.thing == 1)
    p2 = (user.age > 4)

    log_op1 = p1 & p2
    assert isinstance(log_op1, BooleanExpression)
    assert log_op1.op_code == OP_CODE.AND
    assert log_op1.lhs is p1
    assert log_op1.rhs is p2

    log_op2 = p1 | p2
    assert isinstance(log_op2, BooleanExpression)
    assert log_op2.op_code == OP_CODE.OR
    assert log_op2.lhs is p1
    assert log_op2.rhs is p2


def test_ordering_created():
    user = Symbol()

    ordering = user.age.asc
    assert isinstance(ordering, Ordering)
    assert ordering.attr.key == 'age'
    assert not ordering.desc

    ordering = user.age.desc
    assert isinstance(ordering, Ordering)
    assert ordering.attr.key == 'age'
    assert ordering.desc


def test_build_query(store_with_data):
    user = Symbol()
    query = Query(store_with_data)

    ret_query = query.select('name', user.age)
    assert ret_query is query
    assert 'name' in query.selected
    assert 'age' in query.selected
    assert isinstance(query.selected['age'], SymbolicAttribute)
    assert isinstance(query.selected['name'], SymbolicAttribute)

    ret_query = query.order_by(user.age, 'name', user.a.desc, user.b.asc)
    assert ret_query is query
    assert query.orderings[0].attr.key == 'age'
    assert query.orderings[1].attr.key == 'name'
    assert query.orderings[2].attr.key == 'a'
    assert query.orderings[2].desc
    assert query.orderings[3].attr.key == 'b'
    assert not query.orderings[3].desc

    ret_query = query.limit(10)
    assert ret_query is query
    assert query.limit_index == 10

    ret_query = query.offset(100)
    assert ret_query is query
    assert query.offset_index == 100


    pred = (user.age == 1)
    ret_query = query.where(pred)
    assert ret_query is query
    assert query.predicate is pred

    query.predicate = None
    query.where(pred, pred)
    assert ret_query is query
    assert isinstance(query.predicate, BooleanExpression)
    assert query.predicate.op_code == OP_CODE.AND
    assert query.predicate.lhs is pred
    assert query.predicate.rhs is pred

    query.where(pred)
    assert isinstance(query.predicate.rhs, ConditionalExpression)
    assert isinstance(query.predicate.lhs, BooleanExpression)


def test_query_execute_normal(store_with_data, click_event):
    store = store_with_data
    event = store_with_data.symbol()
    query = Query(store).select(
        event.position,
        event.type
    ).where(
        event.type == 'click'
    )

    # execute with first
    event = query.execute(first=True) 
    assert event is not None
    assert event['type'] == 'click'
    assert set(event.keys()) == {'id', 'position', 'type'}

    # execute with not first
    events = query.execute(first=False) 
    assert events
    assert len(events) == 1
    assert click_event['id'] in events

    event = events[click_event['id']]
    assert set(event.keys()) == {'id', 'position', 'type'}


def test_query_execute_with_ordering_normal(store_with_data):
    store = store_with_data
    event = store_with_data.symbol()
    query = Query(store).select(
        event.position,
        event.type
    ).order_by(
        event.type.asc
    )

    events = list(query.execute().values())
    assert len(events) == 2
    assert events[0]['type'] == 'click'
    assert events[1]['type'] == 'press'

    query = Query(store).select(
        event.position,
        event.type
    ).order_by(
        event.type.desc
    )

    events = list(query.execute().values())
    assert len(events) == 2
    assert events[0]['type'] == 'press'
    assert events[1]['type'] == 'click'
