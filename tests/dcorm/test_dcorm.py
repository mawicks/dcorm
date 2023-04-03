from dataclasses import dataclass
import datetime as dt
from typing import Optional, Union
from zoneinfo import ZoneInfo
import sqlite3

from sandbox.dcorm.dcorm import orm

import pytest

SOME_INT = 2
SOME_OTHER_INT = 7

SOME_FLOAT = 1.5
SOME_OTHER_FLOAT = 2.3

SOME_STRING = "foo"
SOME_OTHER_STRING = "bar"
YET_ANOTHER_STRING = "baz"

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


EXPECTED_SQLITE_TYPE = {
    "an_int": "INTEGER",
    "a_float": "REAL",
    "a_str": "TEXT",
    "a_date": "TEXT",
    "a_datetime": "TEXT",
    "a_nullable_int": "INTEGER",
    "a_nullable_float": "REAL",
    "a_nullable_string": "TEXT",
}


class SomeNonDataClass:
    an_int: int


@orm.orm_dataclass
@dataclass
class SomeDataClass:
    an_int: int
    a_float: float
    a_str: str
    a_date: dt.date
    a_datetime: dt.datetime
    a_nullable_int: None | int = None
    a_nullable_float: Optional[float] = None
    a_nullable_string: Union[str, None] = None


@orm.orm_dataclass
@dataclass
class ContainingDataClass:
    a: int
    containee: Optional[SomeDataClass]


@pytest.fixture
def some_instance() -> SomeDataClass:
    return SomeDataClass(
        SOME_INT,
        SOME_FLOAT,
        SOME_STRING,
        SOME_DATE,
        SOME_DATETIME,
        a_nullable_string=YET_ANOTHER_STRING,
    )


@pytest.fixture
def some_other_instance() -> SomeDataClass:
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
    empty_db = sqlite3.connect(":memory:")
    return empty_db


@pytest.fixture
def with_tables_created(connection):
    orm.set_connection_factory(lambda: connection)
    orm.create(SomeDataClass)
    orm.create(ContainingDataClass)
    return connection


@pytest.fixture
def with_one_row_inserted(with_tables_created, some_instance):
    orm.set_connection_factory(lambda: with_tables_created)
    orm.insert(some_instance)
    return with_tables_created


@pytest.fixture
def with_two_rows_inserted(with_one_row_inserted, some_other_instance):
    orm.set_connection_factory(lambda: with_one_row_inserted)
    orm.insert(some_other_instance)
    return with_one_row_inserted


def test_orm_decorated_class_has_orm_returns_true():
    assert orm.has_orm(SomeDataClass) is True


def test_orm_decorated_instance_has_orm_returns_true(some_instance):
    assert orm.has_orm(some_instance) is True


def test_undecorated_class_has_orm_returns_false():
    assert orm.has_orm(SomeNonDataClass) is False


def test_undecorated_instance_has_orm_returns_false():
    assert orm.has_orm(SomeNonDataClass()) is False  # type: ignore


def test_instance_has_expected_fields():
    instance = SomeDataClass(
        SOME_INT,
        SOME_FLOAT,
        SOME_STRING,
        SOME_DATE,
        SOME_DATETIME,
        a_nullable_string=YET_ANOTHER_STRING,
    )
    assert instance.an_int == SOME_INT
    assert instance.a_float == SOME_FLOAT
    assert instance.a_str == SOME_STRING
    assert instance.a_date == SOME_DATE
    assert instance.a_datetime == SOME_DATETIME
    assert instance.a_nullable_float == None
    assert instance.a_nullable_int == None
    assert instance.a_nullable_string == YET_ANOTHER_STRING


def test_create_executes_no_exception(connection: sqlite3.Connection):
    orm.set_connection_factory(lambda: connection)
    orm.create(SomeDataClass)


def test_create_raises_on_non_dataclass(connection: sqlite3.Connection):
    orm.set_connection_factory(lambda: connection)
    with pytest.raises(TypeError):
        orm.create(SomeNonDataClass)


def test_columns_have_expected_db_types(with_tables_created):
    cursor = with_tables_created.execute("PRAGMA table_info(SomeDataClass)")
    for row in cursor:
        d = {
            field_description[0]: field_data
            for field_description, field_data in zip(cursor.description, row)
        }
        assert d["type"] == EXPECTED_SQLITE_TYPE[d["name"]]


def test_create_raises_when_exists(with_tables_created: sqlite3.Connection):
    orm.set_connection_factory(lambda: with_tables_created)
    with pytest.raises(Exception):
        orm.create(SomeDataClass)


def test_create_no_exception_with_drop_if_exists(
    with_tables_created: sqlite3.Connection,
):
    orm.set_connection_factory(lambda: with_tables_created)
    orm.create(SomeDataClass, drop_if_exists=True)


def test_insert(with_tables_created: sqlite3.Connection, some_instance: SomeDataClass):
    orm.set_connection_factory(lambda: with_tables_created)
    id = orm.insert(some_instance)
    assert id is not None


def test_whether_order_matters(connection):
    # This tests whether an implementation detail causes a dependence on order.
    # Because of the potential for forward references, descriptors don't get
    # added until the first *use* of the ORM after the forward references get
    # resolved.  Here we'll create an object with an attribute containing
    # another object *before* using any ORM layers.  The descriptors won't
    # exist when we construct the object.  Make sure the descriptor can find
    # the attribute.  It's also important to create the classes inside
    # this test since classes used in other tests get adjusted before
    # this test runs.  If you don't do this, running other tests can affect
    # whether this test succeeds or not.  It can succeed even in the presence
    # of a bug if other tests run first.
    @orm.orm_dataclass
    @dataclass
    class Foo:
        a: int

    @orm.orm_dataclass
    @dataclass
    class Bar:
        b: int
        foo: Foo

    foo = Foo(a=7)
    bar = Bar(b=11, foo=foo)

    orm.set_connection_factory(lambda: connection)
    orm.create(Foo, drop_if_exists=False)
    orm.create(Bar, drop_if_exists=False)
    id = orm.insert(bar)
    instance = orm.get_by_id(Bar, id)
    assert instance.foo == foo


def test_read_after_insert_returns_expected_record(
    with_tables_created: sqlite3.Connection, some_instance: SomeDataClass
):
    orm.set_connection_factory(lambda: with_tables_created)
    id = orm.insert(some_instance)
    read_instance = orm.get_by_id(SomeDataClass, id)
    assert read_instance == some_instance


def test_update_by_id_modifies_record(
    with_tables_created: sqlite3.Connection,
    some_instance: SomeDataClass,
    some_other_instance: SomeDataClass,
):
    orm.set_connection_factory(lambda: with_tables_created)
    id = orm.insert(some_instance)
    orm.update_by_id(some_other_instance, id)
    read_instance = orm.get_by_id(SomeDataClass, id)
    assert read_instance == some_other_instance


def test_update_modifies_record(
    with_tables_created: sqlite3.Connection,
    some_instance: SomeDataClass,
):
    orm.set_connection_factory(lambda: with_tables_created)
    id = orm.insert(some_instance)

    # Change some fields
    some_instance.an_int = SOME_OTHER_INT
    some_instance.a_float = SOME_OTHER_FLOAT

    # Update the record
    orm.update(some_instance)

    # Read the record back from its original location
    read_instance = orm.get_by_id(SomeDataClass, id)

    # Confirm some change/unchanged fields.
    assert read_instance.an_int == SOME_OTHER_INT
    assert read_instance.a_float == SOME_OTHER_FLOAT
    assert read_instance.a_str == SOME_STRING


def test_read_causes_exception_after_delete_by_id(
    with_tables_created: sqlite3.Connection, some_instance: SomeDataClass
):
    orm.set_connection_factory(lambda: with_tables_created)
    id = orm.insert(some_instance)
    orm.get_by_id(SomeDataClass, id)
    orm.delete_by_id(SomeDataClass, id)
    with pytest.raises(Exception):
        orm.get_by_id(SomeDataClass, id)


def test_read_causes_exception_after_delete(
    with_tables_created: sqlite3.Connection, some_instance: SomeDataClass
):
    orm.set_connection_factory(lambda: with_tables_created)
    id = orm.insert(some_instance)
    orm.delete(some_instance)
    with pytest.raises(Exception):
        orm.get_by_id(SomeDataClass, id)


def test_get_all_returns_two_instances(with_two_rows_inserted):
    orm.set_connection_factory(lambda: with_two_rows_inserted)
    all_instances = list(orm.get_all(SomeDataClass))
    assert len(all_instances) == 2


def test_get_all_instances_of_expected_class(with_two_rows_inserted):
    orm.set_connection_factory(lambda: with_two_rows_inserted)
    all_instances = list(orm.get_all(SomeDataClass))
    assert type(all_instances[0]) is SomeDataClass
    assert type(all_instances[1]) is SomeDataClass


def test_instances_from_get_all_can_be_deleted(with_two_rows_inserted):
    orm.set_connection_factory(lambda: with_two_rows_inserted)
    for instance in orm.get_all(SomeDataClass):
        orm.delete(instance)
    assert len(list(orm.get_all(SomeDataClass))) == 0


def test_inserting_container_also_inserts_containee(
    with_tables_created, containing_instance
):
    orm.set_connection_factory(lambda: with_tables_created)
    orm.insert(containing_instance)
    all_containee_instances = list(orm.get_all(SomeDataClass))
    assert len(all_containee_instances) == 1


def test_inserting_container_doesnt_insert_preexisting_containee(
    with_tables_created, containing_instance
):
    orm.set_connection_factory(lambda: with_tables_created)
    orm.insert(containing_instance.containee)
    orm.insert(containing_instance)
    all_containee_instances = list(orm.get_all(SomeDataClass))
    assert len(all_containee_instances) == 1


def test_containee_autoloads(with_tables_created, containing_instance):
    orm.set_connection_factory(lambda: with_tables_created)
    orm.insert(containing_instance)
    all_container_instances = list(orm.get_all(ContainingDataClass))
    single_instance = all_container_instances[0]
    containee = single_instance.containee
    assert containee is not None and containee.an_int == SOME_INT


def test_can_insert_when_containee_is_none(with_tables_created):
    orm.set_connection_factory(lambda: with_tables_created)
    orm.insert(ContainingDataClass(a=13, containee=None))


def test_can_read_when_containee_is_none(with_tables_created):
    orm.set_connection_factory(lambda: with_tables_created)
    instance = ContainingDataClass(a=13, containee=None)
    id = orm.insert(instance)
    read_instance = orm.get_by_id(ContainingDataClass, id)
    assert instance == read_instance


def test_updating_container_modifies_containee_reference(
    with_tables_created, containing_instance, some_other_instance
):
    orm.set_connection_factory(lambda: with_tables_created)
    id = orm.insert(containing_instance)
    containing_instance.containee = some_other_instance
    orm.update(containing_instance)
    read_container = orm.get_by_id(ContainingDataClass, id)
    assert read_container.containee == some_other_instance
