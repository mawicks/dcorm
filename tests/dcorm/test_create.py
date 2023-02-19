from dataclasses import dataclass
import datetime as dt
from zoneinfo import ZoneInfo
import sqlite3

from sandbox.dcorm import dcorm

import pytest

SOME_INT_VALUE = 2
SOME_FLOAT_VALUE = 1.5
SOME_STRING = "foo"
SOME_DATE = dt.date(year=2023, month=2, day=18)
SOME_UTC_DATETIME = dt.datetime(
    year=2023, month=2, day=18, hour=19, minute=25, tzinfo=dt.timezone.utc
)
SOME_EST_DATETIME = dt.datetime(
    year=2023, month=2, day=18, hour=19, minute=25, tzinfo=ZoneInfo("America/New_York")
)


@dataclass
class SomeDataClass:
    an_int: int
    a_float: float
    a_str: str
    a_date: dt.date
    a_datetime: dt.datetime


class SomeClass:
    an_int: int


@pytest.fixture
def some_instance():
    return SomeDataClass(
        SOME_INT_VALUE, SOME_FLOAT_VALUE, SOME_STRING, SOME_DATE, SOME_UTC_DATETIME
    )


@pytest.fixture
def connection():
    print("Connection Called")
    return sqlite3.connect(":memory:")


@pytest.fixture
def with_table_created(connection):
    dcorm.create(connection, SomeDataClass)
    return connection


@pytest.fixture
def with_one_row_inserted(with_table_created, some_instance):
    dcorm.insert_one(with_table_created, some_instance)
    return with_table_created


def test_create_raises_on_non_dataclass(connection: sqlite3.Connection):
    with pytest.raises(TypeError):
        dcorm.create(connection, SomeClass)


def test_create_executes_no_exception(connection: sqlite3.Connection):
    dcorm.create(connection, SomeDataClass)


def test_create_raises_when_exists(with_table_created: sqlite3.Connection):
    with pytest.raises(Exception):
        dcorm.create(with_table_created, SomeDataClass)


def test_create_no_exception_with_drop_if_exists(
    with_table_created: sqlite3.Connection,
):
    dcorm.create(with_table_created, SomeDataClass, drop_if_exists=True)


def test_insert_one(with_table_created: sqlite3.Connection, some_instance):
    id = dcorm.insert_one(with_table_created, some_instance)
    assert id is not None


def test_query_one(with_table_created: sqlite3.Connection, some_instance):
    id = dcorm.insert_one(with_table_created, some_instance)
    read_instance = dcorm.read_from_id(with_table_created, SomeDataClass, id)
    assert read_instance == some_instance
