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
    # set Bar records pointing at a different
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
