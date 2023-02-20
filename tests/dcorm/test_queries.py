from dataclasses import dataclass
import pytest
import sqlite3

from sandbox.dcorm.queries import Select
from sandbox.dcorm.dcorm import orm_dataclass, create, insert, get_all


@orm_dataclass
@dataclass
class Foo:
    a: int


@orm_dataclass
@dataclass
class Bar:
    b: int
    some_foo: Foo


@pytest.fixture
def empty_db():
    return sqlite3.connect(":memory:")


@pytest.fixture
def tables_created(empty_db):
    create(empty_db, Foo)
    create(empty_db, Bar)
    return empty_db


@pytest.fixture
def relations_inserted(tables_created):
    # Insert two Foo records and
    # Two sets of two Bar records with each
    # set of Bar records pointing at a different
    # Foo record.
    for a in range(2):
        foo = Foo(a=a)
        insert(tables_created, foo)
        for b in range(2):
            bar = Bar(b=b, some_foo=foo)
            insert(tables_created, bar)
    return tables_created


def test_relations_inserted_has_two_foo_records(relations_inserted):
    assert len(list(get_all(relations_inserted, Foo))) == 2


def test_relations_inserted_has_four_bar_records(relations_inserted):
    assert len(list(get_all(relations_inserted, Bar))) == 4


def test_empty_select_foo_returns_two_records(relations_inserted):
    assert len(list(Select(Foo)(relations_inserted))) == 2


def test_empty_select_bar_returns_four_records(relations_inserted):
    assert len(list(Select(Bar)(relations_inserted))) == 4


def test_select_bar_join_foo_returns_four_records(relations_inserted):
    assert len(list(Select(Bar).join("some_foo")(relations_inserted))) == 4


def test_select_bar_join_foo_raises_with_invalid_field_name(relations_inserted):
    with pytest.raises(ValueError):
        Select(Bar).join("wrong_name")


def test_select_bar_join_foo_raises_with_non_dataclass_field(relations_inserted):
    with pytest.raises(ValueError):
        Select(Bar).join("b")


def test_select_bar_join_foo_where_specific_foo_returns_two_records(relations_inserted):
    query = Select(Bar).join("some_foo").where("some_foo.a = 1")
    results = list(query(relations_inserted))
    assert len(results) == 2


def test_select_bar_join_foo_where_specific_foo_returns_correct_records(
    relations_inserted,
):
    query = Select(Bar).join("some_foo").where("some_foo.a = 1")
    results = list(query(relations_inserted))
    assert results[0].some_foo.a == 1
    assert results[1].some_foo.a == 1
