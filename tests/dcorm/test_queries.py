from dataclasses import dataclass
import pytest
import sqlite3
from typing import cast

from sandbox.dcorm import orm


@orm.orm_dataclass
@dataclass
class Foo:
    a: int


@orm.orm_dataclass
@dataclass
class Bar:
    b: int
    some_foo: Foo


@pytest.fixture
def empty_db():
    return sqlite3.connect(":memory:")


@pytest.fixture
def tables_created(empty_db):
    orm.set_connection_factory(lambda: empty_db)
    orm.create(Foo)
    orm.create(Bar)
    return empty_db


@pytest.fixture
def relations_inserted(tables_created):
    orm.set_connection_factory(lambda: tables_created)
    # Insert two Foo records and
    # Two sets of two Bar records with each
    # set of Bar records pointing at a different
    # Foo record.
    for a in range(2):
        foo = Foo(a=a)
        orm.insert(foo)
        for b in range(2):
            bar = Bar(b=b, some_foo=foo)
            orm.insert(bar)
    return tables_created


def test_relations_inserted_has_two_foo_records(relations_inserted):
    orm.set_connection_factory(lambda: relations_inserted)
    assert len(list(orm.get_all(Foo))) == 2


def test_relations_inserted_has_four_bar_records(relations_inserted):
    orm.set_connection_factory(lambda: relations_inserted)
    assert len(list(orm.get_all(Bar))) == 4


def test_empty_select_foo_returns_two_records(relations_inserted):
    orm.set_connection_factory(lambda: relations_inserted)
    assert len(list(orm.select(Foo)())) == 2


def test_empty_select_bar_returns_four_records(relations_inserted):
    orm.set_connection_factory(lambda: relations_inserted)
    assert len(list(orm.select(Bar)())) == 4


def test_select_bar_join_foo_returns_four_records(relations_inserted):
    orm.set_connection_factory(lambda: relations_inserted)
    assert len(list(orm.select(Bar).join("some_foo")())) == 4


def test_select_bar_join_foo_raises_with_invalid_field_name():
    with pytest.raises(ValueError):
        orm.select(Bar).join("wrong_name")


def test_select_bar_join_foo_raises_with_non_dataclass_field():
    with pytest.raises(ValueError):
        orm.select(Bar).join("b")


def test_select_bar_join_foo_where_specific_foo_returns_two_records(relations_inserted):
    orm.set_connection_factory(lambda: relations_inserted)
    query = orm.select(Bar).join("some_foo").where("some_foo.a = 1")
    results = list(query())
    assert len(results) == 2


def test_select_bar_join_foo_by_joining_select(relations_inserted):
    orm.set_connection_factory(lambda: relations_inserted)
    selected_foo = orm.select(Foo).where("Foo.a = 1")
    query = orm.select(Bar).join("some_foo", selected_foo)
    results = list(query())
    assert len(results) == 2


def test_select_bar_join_bar_on_some_foo_raises_type_error():
    selected_bar = orm.select(Bar).where("Bar.b = 1")
    # This should raise a type error because some_foo has type Foo
    # but we're joining it on a Select(Bar)
    with pytest.raises(TypeError):
        orm.select(Bar).join("some_foo", selected_bar)


def test_select_bar_join_foo_where_specific_foo_returns_correct_records(
    relations_inserted,
):
    orm.set_connection_factory(lambda: relations_inserted)
    query = orm.select(Bar).join("some_foo").where("some_foo.a = 1")
    results = cast(list[Bar], list(query()))
    assert results[0].some_foo.a == 1
    assert results[1].some_foo.a == 1
