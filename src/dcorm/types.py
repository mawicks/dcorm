from __future__ import annotations
import datetime as dt
import dataclasses
from typing import Any, Callable, Iterable, Tuple, TypeVar, Protocol

# Type definitions


class DataClass(Protocol):
    # Taken from https://stackoverflow.com/questions/74153330/python-how-to-type-hint-a-dataclass
    __dict__: dict[str, Any]
    __doc__: str | None
    # if using `@dataclass(slots=True)`
    __slots__: str | Iterable[str]
    __annotations__: dict[str, str | type]
    __dataclass_fields__: dict[str, dataclasses.Field]
    # the actual class definition is marked as private, and here I define
    # it as a forward reference, as I don't want to encourage
    # importing private or "unexported" members.
    #  __dataclass_params__: "_DataclassParams"
    # __post_init__: Callable | None


DataClassType = type
SomeDataClassType = TypeVar("SomeDataClassType", bound=type)
SomeDataClass = TypeVar("SomeDataClass", bound=DataClass)
SQLParameter = int | float | str | dt.datetime | dt.date | DataClass


class Cursor(Protocol):
    @property
    def lastrowid(self) -> int | None:
        ...

    def execute(self, sql: str, parameters: Tuple[object, ...] = (), /) -> Cursor:
        ...

    def executemany(
        self, sql: str, parameters: Iterable[Tuple[object, ...]], /
    ) -> Cursor:
        ...

    def executescript(self, sql: str, /) -> Cursor:
        ...

    def fetchall(self) -> list[tuple[object]]:
        ...


class Connection(Protocol):
    def cursor(self, cursorClass: None = None) -> Cursor:
        ...

    def commit(self):
        ...

    def rollback(self):
        ...


ConnectionFactory = Callable[[], Connection]


class KeyType(int):
    pass


# Type definitions
@dataclasses.dataclass
class Field:
    name: str
    type_hint: type
    non_null_type: type
