from __future__ import annotations
from dataclasses import dataclass
import sqlite3
from typing import cast

from dcorm import orm
from dcorm.types import Connection

import pytest


@orm.orm_dataclass
@dataclass
class SelfReference:
    name: str
    parent: SelfReference | None = None
    a_nullable_int: int | None = None
    a_nullable_int_with_integer_default: int | None = 42


def test_instance_has_expected_default_values():
    instance = SelfReference("foo")
    assert instance.name == "foo"
    assert instance.parent == None
    assert instance.a_nullable_int == None
    assert instance.a_nullable_int_with_integer_default == 42


def test_instance_has_expected_set_values():
    instance = SelfReference(
        "foo",
        a_nullable_int=10,
        a_nullable_int_with_integer_default=19,
    )
    assert instance.name == "foo"
    assert instance.parent == None
    assert instance.a_nullable_int == 10
    assert instance.a_nullable_int_with_integer_default == 19


@pytest.fixture
def connection():
    empty_db = sqlite3.connect(":memory:")
    return empty_db


@pytest.fixture
def with_tables_created(connection):
    orm.set_connection_factory(lambda: connection)
    orm.create(SelfReference)
    return connection


@pytest.fixture
def with_parent_and_child_inserted(with_tables_created):
    parent = SelfReference(name="parent")
    child = SelfReference(name="child")
    child.parent = parent

    # This should insert both the parent and the child
    orm.set_connection_factory(lambda: with_tables_created)
    orm.insert(child)
    return with_tables_created


def test_orm_decorated_class_has_orm_returns_true():
    assert orm.has_orm(SelfReference) is True


def test_create_executes_no_exception(connection: Connection):
    orm.set_connection_factory(lambda: connection)
    orm.create(SelfReference)


def test_inserted_child_is_retrievable(with_tables_created: sqlite3.Connection):
    parent = SelfReference(name="parent")
    child = SelfReference(name="child")
    child.parent = parent

    # This should insert both the parent and the child
    orm.set_connection_factory(lambda: with_tables_created)
    id = orm.insert(child)
    instance = orm.get_by_id(SelfReference, id)
    assert instance.name == "child"  # type: ignore


def test_with_parent_and_child_inserted_has_two_records(with_parent_and_child_inserted):
    orm.set_connection_factory(lambda: with_parent_and_child_inserted)
    all_instances = list(orm.get_all(SelfReference))
    assert len(all_instances) == 2


def test_can_select_child(with_parent_and_child_inserted):
    orm.set_connection_factory(lambda: with_parent_and_child_inserted)
    query = orm.select(SelfReference).where("name = ?", ("child",))
    child = cast(SelfReference, list(query())[0])
    assert child.name == "child"


def test_queried_child_can_resolve_parent(with_parent_and_child_inserted):
    orm.set_connection_factory(lambda: with_parent_and_child_inserted)
    query = orm.select(SelfReference).where("name = ?", ("child",))
    child = cast(SelfReference, list(query())[0])
    assert child.parent is not None and child.parent.name == "parent"
