from __future__ import annotations
from dataclasses import dataclass
import sqlite3

from sandbox.dcorm import dcorm
from sandbox.dcorm.queries import Select

import pytest


@dcorm.orm_dataclass
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
    return sqlite3.connect(":memory:")


@pytest.fixture
def with_tables_created(connection):
    dcorm.create(connection, SelfReference)
    return connection


@pytest.fixture
def with_parent_and_child_inserted(with_tables_created):
    parent = SelfReference(name="parent")
    child = SelfReference(name="child")
    child.parent = parent

    # This should insert both the parent and the child
    dcorm.insert(with_tables_created, child)
    return with_tables_created


def test_orm_decorated_class_has_orm_returns_true():
    assert dcorm.has_orm(SelfReference) is True


def test_create_executes_no_exception(connection: sqlite3.Connection):
    dcorm.create(connection, SelfReference)


def test_inserted_child_is_retrievable(with_tables_created: sqlite3.Connection):
    parent = SelfReference(name="parent")
    child = SelfReference(name="child")
    child.parent = parent

    # This should insert both the parent and the child
    id = dcorm.insert(with_tables_created, child)
    instance = dcorm.get_by_id(with_tables_created, SelfReference, id)
    assert instance.name == "child"  # type: ignore


def test_with_parent_and_child_inserted_has_two_records(with_parent_and_child_inserted):
    all_instances = list(dcorm.get_all(with_parent_and_child_inserted, SelfReference))
    assert len(all_instances) == 2


def test_can_select_child(with_parent_and_child_inserted):
    query = Select(SelfReference).where("name = ?", ("child",))
    child = list(query(with_parent_and_child_inserted))[0]
    assert child.name == "child"


def test_queried_child_can_resolve_parent(with_parent_and_child_inserted):
    query = Select(SelfReference).where("name = ?", ("child",))
    child = list(query(with_parent_and_child_inserted))[0]
    assert child.parent.name == "parent"
