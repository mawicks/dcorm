from dataclasses import dataclass
import datetime as dt
from typing import Optional
from zoneinfo import ZoneInfo
import sqlite3

from sandbox.dcorm import dcorm

import pytest

SOME_INT = 2
SOME_OTHER_INT = 7

SOME_FLOAT = 1.5
SOME_OTHER_FLOAT = 2.3

SOME_STRING = "foo"
SOME_OTHER_STRING = "bar"

SOME_DATE = dt.date(year=2023, month=2, day=18)
SOME_OTHER_DATE = dt.date(year=2022, month=1, day=10)

SOME_UTC_DATETIME = dt.datetime(
    year=2023, month=2, day=18, hour=19, minute=25, tzinfo=dt.timezone.utc
)
SOME_OTHER_UTC_DATETIME = dt.datetime(
    year=2022, month=9, day=15, hour=10, minute=25, tzinfo=dt.timezone.utc
)
SOME_DATETIME = dt.datetime(
    year=2023, month=2, day=18, hour=19, minute=25, tzinfo=ZoneInfo("America/New_York")
)

SOME_OTHER_DATETIME = dt.datetime(
    year=2023, month=9, day=15, hour=10, minute=25, tzinfo=ZoneInfo("America/New_York")
)


class SomeNonDataClass:
    an_int: int


@dcorm.orm_dataclass
@dataclass
class SomeDataClass:
    an_int: int
    a_float: float
    a_str: str
    a_date: dt.date
    a_datetime: dt.datetime
    a_nullable_int: None | int = None


@dcorm.orm_dataclass
@dataclass
class ContainingDataClass:
    a: int
    containee: SomeDataClass


@pytest.fixture
def some_instance():
    return SomeDataClass(SOME_INT, SOME_FLOAT, SOME_STRING, SOME_DATE, SOME_DATETIME)


@pytest.fixture
def some_other_instance():
    return SomeDataClass(
        SOME_OTHER_INT,
        SOME_OTHER_FLOAT,
        SOME_OTHER_STRING,
        SOME_OTHER_DATE,
        SOME_OTHER_DATETIME,
    )


@pytest.fixture
def containing_instance(some_instance):
    return ContainingDataClass(a=13, containee=some_instance)


@pytest.fixture
def connection():
    connection = sqlite3.connect(":memory:")
    # connection.row_factory = dict_factory
    return connection


@pytest.fixture
def with_tables_created(connection):
    dcorm.create(connection, SomeDataClass)
    dcorm.create(connection, ContainingDataClass)
    return connection


@pytest.fixture
def with_one_row_inserted(with_tables_created, some_instance):
    dcorm.insert(with_tables_created, some_instance)
    return with_tables_created


@pytest.fixture
def with_two_rows_inserted(with_one_row_inserted, some_other_instance):
    dcorm.insert(with_one_row_inserted, some_other_instance)
    return with_one_row_inserted


def test_orm_decorated_class_has_orm_returns_true():
    assert dcorm.has_orm(SomeDataClass) is True


def test_orm_decorated_instance_has_orm_returns_true(some_instance):
    assert dcorm.has_orm(some_instance) is True


def test_undecorated_class_has_orm_returns_false():
    assert dcorm.has_orm(SomeNonDataClass) is False


def test_undecorated_instance_has_orm_returns_false():
    assert dcorm.has_orm(SomeNonDataClass()) is False


def test_create_raises_on_non_dataclass(connection: sqlite3.Connection):
    with pytest.raises(TypeError):
        dcorm.create(connection, SomeNonDataClass)


def test_create_executes_no_exception(connection: sqlite3.Connection):
    dcorm.create(connection, SomeDataClass)


def test_create_raises_when_exists(with_tables_created: sqlite3.Connection):
    with pytest.raises(Exception):
        dcorm.create(with_tables_created, SomeDataClass)


def test_create_no_exception_with_drop_if_exists(
    with_tables_created: sqlite3.Connection,
):
    dcorm.create(with_tables_created, SomeDataClass, drop_if_exists=True)


def test_insert(with_tables_created: sqlite3.Connection, some_instance: SomeDataClass):
    id = dcorm.insert(with_tables_created, some_instance)
    assert id is not None


def test_read_after_insert_returns_expected_record(
    with_tables_created: sqlite3.Connection, some_instance: SomeDataClass
):
    id = dcorm.insert(with_tables_created, some_instance)
    read_instance = dcorm.get_by_id(with_tables_created, SomeDataClass, id)
    assert read_instance == some_instance


def test_update_by_id_modifies_record(
    with_tables_created: sqlite3.Connection,
    some_instance: SomeDataClass,
    some_other_instance: SomeDataClass,
):
    id = dcorm.insert(with_tables_created, some_instance)
    dcorm.update_by_id(with_tables_created, some_other_instance, id)
    read_instance = dcorm.get_by_id(with_tables_created, SomeDataClass, id)
    assert read_instance == some_other_instance


def test_update_modifies_record(
    with_tables_created: sqlite3.Connection,
    some_instance: SomeDataClass,
):
    id = dcorm.insert(with_tables_created, some_instance)

    # Change some fields
    some_instance.an_int = SOME_OTHER_INT
    some_instance.a_float = SOME_OTHER_FLOAT

    # Update the record
    dcorm.update(with_tables_created, some_instance)

    # Read the record back from it's original location
    read_instance = dcorm.get_by_id(with_tables_created, SomeDataClass, id)

    # Confirm some change/unchanged fields.
    assert read_instance.an_int == SOME_OTHER_INT
    assert read_instance.a_float == SOME_OTHER_FLOAT
    assert read_instance.a_str == SOME_STRING


def test_read_causes_exception_after_delete_by_id(
    with_tables_created: sqlite3.Connection, some_instance: SomeDataClass
):
    id = dcorm.insert(with_tables_created, some_instance)
    dcorm.get_by_id(with_tables_created, SomeDataClass, id)
    dcorm.delete_by_id(with_tables_created, SomeDataClass, id)
    with pytest.raises(Exception):
        dcorm.get_by_id(with_tables_created, SomeDataClass, id)


def test_read_causes_exception_after_delete(
    with_tables_created: sqlite3.Connection, some_instance: SomeDataClass
):
    id = dcorm.insert(with_tables_created, some_instance)
    dcorm.delete(with_tables_created, some_instance)
    with pytest.raises(Exception):
        dcorm.get_by_id(with_tables_created, SomeDataClass, id)


def test_get_all_returns_two_instances(with_two_rows_inserted):
    all_instances = list(dcorm.get_all(with_two_rows_inserted, SomeDataClass))
    assert len(all_instances) == 2


def test_get_all_instances_of_expected_class(with_two_rows_inserted):
    all_instances = list(dcorm.get_all(with_two_rows_inserted, SomeDataClass))
    assert type(all_instances[0]) is SomeDataClass
    assert type(all_instances[1]) is SomeDataClass


def test_instances_from_get_all_can_be_deleted(with_two_rows_inserted):
    for instance in dcorm.get_all(with_two_rows_inserted, SomeDataClass):
        dcorm.delete(with_two_rows_inserted, instance)
    assert len(list(dcorm.get_all(with_two_rows_inserted, SomeDataClass))) == 0


def test_inserting_container_also_inserts_containee(
    with_tables_created, containing_instance
):
    dcorm.insert(with_tables_created, containing_instance)
    all_containee_instances = list(dcorm.get_all(with_tables_created, SomeDataClass))
    assert len(all_containee_instances) == 1


def test_inserting_container_doesnt_insert_preexisting_containee(
    with_tables_created, containing_instance
):
    dcorm.insert(with_tables_created, containing_instance.containee)
    dcorm.insert(with_tables_created, containing_instance)
    all_containee_instances = list(dcorm.get_all(with_tables_created, SomeDataClass))
    assert len(all_containee_instances) == 1


def test_updating_container_modifies_containee_reference(
    with_tables_created, containing_instance, some_other_instance
):
    id = dcorm.insert(with_tables_created, containing_instance)
    containing_instance.containee = some_other_instance
    dcorm.update(with_tables_created, containing_instance)
    read_container = dcorm.get_by_id(with_tables_created, ContainingDataClass, id)
    assert read_container.containee == some_other_instance


def test_containee_autoloads(with_tables_created, containing_instance):
    dcorm.insert(with_tables_created, containing_instance)
    all_container_instances = list(
        dcorm.get_all(with_tables_created, ContainingDataClass)
    )
    single_instance = all_container_instances[0]
    containee = single_instance.containee
    assert containee.an_int == SOME_INT
