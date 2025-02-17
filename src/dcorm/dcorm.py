from __future__ import annotations
from collections import defaultdict
import dataclasses
import datetime as dt
import sqlite3
from types import NoneType, UnionType
from typing import Any, Iterator, Tuple, get_type_hints, get_args

import cattr

from dcorm.weak_refs import WeakKeyDict
from dcorm.converters import register_converters

register_converters()


from dcorm.connection_pool import ConnectionPool, ConnectionContextMgr
from dcorm.types import (
    Connection,
    ConnectionFactory,
    DataClass,
    DataClassType,
    Field,
    KeyType,
    SomeDataClass,
    SomeDataClassType,
    SQLParameter,
)

SELF_ROW_ID = "__self__row_id__"
SQLITE_ROWID = "_rowid_"

# The SQLite datatypes are NULL, INTEGER, REAL, TEXT, and BLOB
# The SQLite "affinities" are TEXT, NUMERIC, INTEGER, REAL, and BLOB
# SQLite will resort to NUMERIC automatically for values that can't be sotred
# as REAL or INTEGER, so we use REAL or INTEGER here and let the db use NUMERIC
# when appropriate.  Dates and timestamps are stored in Unix epoch format

SQLITE_TYPE: dict[type, str] = defaultdict(lambda: "BLOB")
SQLITE_TYPE.update(
    {
        int: "INTEGER",
        float: "REAL",
        str: "TEXT",
        dt.date: "TEXT",
        dt.datetime: "TEXT",
    }
)

UTC = dt.timezone.utc


class ORM:
    sql_converter: cattr.Converter
    registered_classes: dict[type, dict[str, object]]
    known_objects: WeakKeyDict

    def __init__(self):
        self.sql_converter = cattr.Converter()
        self.sql_converter.register_structure_hook(
            dt.date, lambda v, _: dt.date.fromisoformat(v)
        )
        self.sql_converter.register_structure_hook(
            # After loading, convert to localtime by calling astimezone() with no arguments.
            dt.datetime,
            lambda v, _: dt.datetime.fromisoformat(v).astimezone(),
        )
        self.sql_converter.register_unstructure_hook(dt.date, lambda d: d.isoformat())

        # Call astimezone() with no arguments to add the timezone if it's
        # missing, otherwise it converts to localtime.  Then convert to UTC.
        self.sql_converter.register_unstructure_hook(
            dt.datetime,
            lambda d: d.astimezone().astimezone(UTC).isoformat(),
        )

        self.registered_classes = {}
        self.known_objects = WeakKeyDict()

    def set_connection_factory(self, connection_factory: ConnectionFactory):
        self.__CONNECTION_POOL__ = ConnectionPool(connection_factory)

    def connection_context(
        self,
        connection: ConnectionContextMgr | None = None,
    ) -> ConnectionContextMgr:
        if connection is None:
            connection = self.__CONNECTION_POOL__.use()
        return connection

    def orm_dataclass(self, cls: SomeDataClassType) -> SomeDataClassType:
        """
        Decorator that registers a class for use with ORM.
        Args:
            cls (DataClassType): The class to register for use with ORM.

        Returns:
            the passed in class
        """
        # This function only registers the class.  It doesn't
        # examine any fields or annotations primarily because of
        # the possibility of forward references.  The class will
        # be prepped for use with ORM the first time it's used with ORM.
        self.registered_classes[cls] = {"has_descriptors": False}
        self.sql_converter.register_structure_hook(cls, lambda v, _: KeyType(v))
        self.sql_converter.register_unstructure_hook(cls, self.get_rowid)
        return cls

    def has_orm(self, instance_or_class: DataClass | DataClassType) -> bool:
        return get_class(instance_or_class) in self.registered_classes

    def create(
        self,
        cls: DataClassType,
        connection: ConnectionContextMgr | None = None,
        drop_if_exists: bool = False,
    ):
        table, fields = self._get_class_fields(cls)

        # SQLite3 generates a rowid automatically so we don't
        # need to create an explicit primary key.
        # Only create the user-defined fields

        column_spec_template = "{name} {datatype}"

        def map_type(field_type):
            if self.has_orm(field_type):
                return KeyType
            else:
                return field_type

        column_specs = ", ".join(
            [
                column_spec_template.format(
                    name=_.name, datatype=SQLITE_TYPE[map_type(_.non_null_type)]
                )
                for _ in fields
            ]
        )

        statements = []
        if drop_if_exists:
            statements.append(f"DROP TABLE IF EXISTS {table}")

        statements.append(f"CREATE TABLE {table} ({column_specs})")

        with self.connection_context(connection) as con:
            cursor = con.cursor()
            cursor.executescript("; ".join(statements))

        return

    def insert(
        self, instance: DataClass, connection: ConnectionContextMgr | None = None
    ) -> KeyType:
        table, fields = get_instance_fields(instance)
        columns = comma_separated_names(fields, include_rowid=False)
        values = ", ".join(["?" for _ in fields])

        parameters = self._astuple(instance, fields)

        with self.connection_context(connection) as con:
            row_id = execute_with_parameters(
                con, f"INSERT INTO {table} ({columns}) VALUES ({values})", parameters
            )

        if row_id is None:
            raise RuntimeError("Row insertion failed to return row_id")

        self._get_dcorm_state(instance)[SELF_ROW_ID] = row_id
        return row_id

    def get_by_id(
        self,
        cls: type[SomeDataClass],
        id: KeyType,
        connection: ConnectionContextMgr | None = None,
    ) -> SomeDataClass:
        table, fields = self._get_class_fields(cls)
        columns = comma_separated_names(fields)

        query = f"SELECT {columns} FROM {table} WHERE {SQLITE_ROWID} = ?"

        with self.connection_context(connection) as con:
            cursor = con.cursor()
            cursor.execute(query, (id,))
            data = cursor.fetchall()
        return next(iter(self._query_results_to_instances(data, cls, fields)))

    def get_all(
        self,
        cls: type[SomeDataClass],
        connection: ConnectionContextMgr | None = None,
    ) -> Iterator[SomeDataClass]:
        table, fields = self._get_class_fields(cls)
        columns = comma_separated_names(fields)

        query = f"SELECT {columns} FROM {table}"

        with self.connection_context(connection) as con:
            cursor = con.cursor()
            cursor.execute(query)
            result = self._query_results_to_instances(cursor.fetchall(), cls, fields)

        return result

    def update_by_id(
        self,
        instance: DataClass,
        id: KeyType,
        connection: ConnectionContextMgr | None = None,
    ):
        table, fields = get_instance_fields(instance)
        column_changes = ", ".join([f"{_.name} = ?" for _ in fields])

        query = f"UPDATE {table} SET {column_changes} WHERE {SQLITE_ROWID} = ?"

        parameters = self._astuple(instance, fields) + (id,)

        with self.connection_context(connection) as con:
            execute_with_parameters(con, query, parameters)

    def get_rowid(self, instance: DataClass) -> KeyType:
        """Return the rowid associated with the instance in the database or raise an exception if it
        isn't known.

        Args:
            instance (Dataclass): The instance

        Raises:
            ValueError: Raised if the instance doesn't have a rowid

        Returns:
            KeyType: The rowid
        """
        rowid = self._get_dcorm_state(instance)[SELF_ROW_ID]

        if rowid is None:
            raise ValueError(f"{instance} hasn't been stored in the database")

        return rowid

    def update(
        self, instance: DataClass, connection: ConnectionContextMgr | None = None
    ):
        self.update_by_id(instance, self.get_rowid(instance), connection)

    def delete_by_id(
        self,
        cls: DataClassType,
        id: KeyType,
        connection: ConnectionContextMgr | None = None,
    ):
        table, _ = self._get_class_fields(cls)

        query = f"DELETE FROM {table} WHERE {SQLITE_ROWID} = ?"

        with self.connection_context(connection) as con:
            execute_with_parameters(con, query, (id,))

    def delete(
        self, instance: DataClass, connection: ConnectionContextMgr | None = None
    ):
        self.delete_by_id(type(instance), self.get_rowid(instance), connection)

    def _get_dcorm_state(self, instance: DataClass) -> dict[str, Any]:
        if instance not in self.known_objects:
            self.known_objects[instance] = {SELF_ROW_ID: None}
        return self.known_objects[instance]

    def select(self, cls: DataClassType):
        return Select(self, cls)

    def _query_results_to_instances(
        self, query_result, cls: type[SomeDataClass], fields
    ) -> Iterator[SomeDataClass]:
        for data in query_result:
            rowid = data[0]
            field_data = data[1:]

            type_hints = get_type_hints(cls)
            converted = [
                self.sql_converter.structure(datum, type_hints[field.name])
                for datum, field in zip(field_data, fields)
            ]
            instance = cls(*converted)
            self._get_dcorm_state(instance)[SELF_ROW_ID] = KeyType(rowid)
            yield instance

    def _add_descriptors_if_missing(self, cls, fields):
        if not self.registered_classes[cls]["has_descriptors"]:
            for field in fields:
                if self.has_orm(field.non_null_type):
                    # If there was a default value set, copy it into the
                    # ForeignReference descriptor class
                    has_default = hasattr(cls, field.name)
                    default = getattr(cls, field.name, None)
                    setattr(
                        cls,
                        field.name,
                        Reference(
                            field.name, field.non_null_type, self, has_default, default
                        ),
                    )
            self.registered_classes[cls]["has_descriptors"] = True
        return

    def _astuple(
        self,
        instance: DataClass,
        fields: tuple[Field, ...],
    ) -> tuple[Any, ...]:
        def values(fields: tuple[Field, ...]):
            for field in fields:
                field_value = getattr(instance, field.name)
                if field_value is not None and self.has_orm(field.non_null_type):
                    # Because this field has ORM, on a read, it is assumed
                    # to be a rowid in the the table associated with
                    # non_null_type. To preserve the validity of this pointer,
                    # don't write anything in this field other than a reference
                    # to the type expected by the type hint or the value None.
                    if isinstance(field_value, field.non_null_type):
                        rowid = self._get_dcorm_state(field_value)[SELF_ROW_ID]
                        if rowid is None:
                            rowid = self.insert(field_value)
                        yield rowid
                    else:
                        raise TypeError(
                            f"{field.name} in {type(instance)} is "
                            f"{type(field_value)} but should be {field.non_null_type}"
                        )
                else:
                    yield field_value

        return tuple(values(fields))

    def _get_class_fields(self, cls: DataClassType) -> tuple[str, tuple[Field, ...]]:
        if not isinstance(cls, type):
            raise TypeError(f"{cls} is not a type")

        if cls not in self.registered_classes:
            raise TypeError(f"{cls} has not been registered with use with ORM")

        table, fields = get_instance_fields(cls)

        # This is a convenient place to add descriptors if they don't already
        # exist.  This should be done after all forward references have been
        # resolved.  If the previous call to get_instance_fields() returned
        # successfully, then the forward references have been resolved.
        self._add_descriptors_if_missing(cls, fields)

        return table, fields


class Reference:
    """
    Descriptor that loads a record from the database using its primary key
    and creates the corresponding object when an attribute is dereferenced.
    """

    name: str
    cls: DataClassType
    orm: ORM
    has_default: bool
    default: Any

    def __init__(
        self,
        name: str,
        cls: DataClassType,
        orm: ORM,
        has_default: bool = False,
        default_value=None,
    ):
        self.name = name
        self.cls = cls
        self.orm = orm
        self.has_default = has_default
        self.default = default_value

    def fix_preexisting(self, instance, dcorm_state):
        # Check for an attribute that may have pre-existed *when* this
        # descriptor was added.  If it exists move it to dcorm_state
        # and remove it from __dict__.
        if (pre_existing := instance.__dict__.get(self.name)) is not None:
            dcorm_state[self.name] = pre_existing
            del instance.__dict__[self.name]

    def __get__(self, instance, cls):
        # The state of contained classes and foreign keys
        # is contained in the dcorm_state dictionary on the instance.
        # The dcorm_state dictionary on the referring class contains
        # the most recently used connection object for that class.
        # Will that always exist?  Since we're dereferencing a record
        # id containing in the referring class, that record ID must have bene
        # read from the database.

        dcorm_state = self.orm._get_dcorm_state(instance)
        self.fix_preexisting(instance, dcorm_state)

        if self.has_default:
            value_or_key = dcorm_state.get(self.name, self.default)
        else:
            value_or_key = dcorm_state[self.name]

        if isinstance(value_or_key, KeyType):
            # value is a foreign key that needs to be dereferenced
            return self.orm.get_by_id(self.cls, value_or_key)
        else:
            return value_or_key

    def __set__(self, instance, value):
        dcorm_state = self.orm._get_dcorm_state(instance)
        self.fix_preexisting(instance, dcorm_state)
        dcorm_state[self.name] = value

    def __delete__(self, instance):
        dcorm_state = self.orm._get_dcorm_state(instance)
        self.fix_preexisting(instance, dcorm_state)
        del dcorm_state[self.name]


class Select:
    orm: ORM
    dataclass: DataClassType
    table: str
    fields: tuple[Field, ...]

    # All of the join lists must have the same length
    join_attributes: list[str]
    join_aliases: list[str]
    join_tables: list[str]
    join_other_attributes: list[str]
    join_other_aliases: list[str]

    where_clauses: list[str]
    parameters: list[Any]

    def __init__(self, orm: ORM, dataclass: DataClassType):
        self.orm = orm
        self.dataclass = dataclass
        self.table, self.fields = orm._get_class_fields(dataclass)
        self.join_attributes = []
        self.join_tables = []
        self.join_aliases = []
        self.join_other_attributes = []
        self.where_clauses = []
        self.parameters = []

    def join(
        self,
        attribute: str | None,
        dataclass_or_select: DataClass | Select | None = None,
        other_attribute: str | None = None,
    ) -> Select:
        validate_join_arguments(attribute, dataclass_or_select, other_attribute)

        # `other_class` is the class/table we're joining to.  It's either an explicitly
        # passed class or it's the class of the Select that's passed.  Below,
        # we'll use the word `class` to indicate the class/table we're joining
        # too.  There's also a join `type` which is the type of the field we're
        # using for the join.  Typically the type is one of the two Classes
        # involve, but we also allow joins on self.Class.a = OtherClass.b
        # where a and b can be any consistent type.  When the attribute is not
        # specified the `type` is the `class`

        # Basically when either attribute is None, it defaults to the primary
        # key/rowid.  When the other class is None, it defaults to the
        # type of the field pointed to by the first attribute, however,
        # we don't allow the other attribute to be used in this case.

        # Most of these calls have side effect on self.join_attributes,
        # self.join_tables, self.join_alias, and self.join_other_attributes.
        left_type = self._get_left_join_type(attribute)
        other_class = self._get_other_join_class(dataclass_or_select, left_type)
        self._handle_right_join_type(left_type, other_class, other_attribute)

        join_table = self._get_join_table(other_class)

        # Set an alias for the table/select being joined.  The select *must*
        # have an alias!
        self.join_aliases.append(attribute if attribute else join_table)
        self._add_to_join_table(join_table, dataclass_or_select)

        return self

    def where(
        self,
        where_clause: str,
        parameters: tuple[SQLParameter, ...] = (),
    ) -> Select:
        self.where_clauses.append(f"({where_clause})")
        self.parameters.extend(
            (self.orm.sql_converter.unstructure(parameter) for parameter in parameters)
        )
        return self

    def where_equal(self, attribute: str, other: Any) -> Select:
        field = find_field(attribute, self.dataclass)
        if type(other) == field.type:
            converted_other = self.orm.sql_converter.unstructure(other)
            self.where_clauses.append(f"{attribute} = {converted_other}")
        else:
            raise ValueError(f"Types {field.type} and {type(other)} are not compatible")

        return self

    def get_statement(self) -> str:
        if len(self.join_attributes) > 0:
            has_join = True
        else:
            has_join = False

        columns = comma_separated_names(
            self.fields, table=(self.table if has_join else None)
        )

        join_list = []
        for table, alias, attribute, other_attribute in zip(
            self.join_tables,
            self.join_aliases,
            self.join_attributes,
            self.join_other_attributes,
        ):
            join_list.append(
                f"JOIN {table} {alias} "
                f"ON {self.table}.{attribute} = {alias if alias else table}.{other_attribute}"
            )
        joins = " ".join(join_list)

        where_clause = " AND ".join(self.where_clauses)
        where = "WHERE" if where_clause else ""

        if has_join:
            distinct = "DISTINCT"
        else:
            distinct = ""

        select = f"SELECT {distinct} {columns} FROM {self.table} {joins} {where} {where_clause}"
        return select

    # Determine the left type:
    def _get_left_join_type(self, attribute: str | None) -> DataClassType:
        if attribute is None:
            self.join_attributes.append(SQLITE_ROWID)
            left_type = self.dataclass
        else:
            self.join_attributes.append(attribute)
            field = find_field(attribute, self.dataclass)
            left_type = field.type
        return left_type

    def _get_other_join_class(
        self,
        dataclass_or_select: DataClass | Select | None,
        left_type: DataClassType,
    ) -> DataClassType:
        # Determine the other class (which determines the table)
        if dataclass_or_select is not None and isinstance(dataclass_or_select, Select):
            other_class = dataclass_or_select.dataclass
        else:
            if dataclass_or_select is None:
                # If no other class was specified, then it's the left_type
                other_class = left_type
            else:
                other_class = type(dataclass_or_select)
            # At this point other_class is not None, so we know the
            # lass/table we're joining to. Confirm that it's using
            # ORM, which means we can get a table name.
        return other_class

    def _get_join_table(self, other_class: DataClassType) -> str:
        try:
            join_table, _ = self.orm._get_class_fields(other_class)
        except TypeError:
            raise ValueError(f"{other_class} is not an ORM dataclass")
        return join_table

    def _add_to_join_table(
        self, join_table: str, dataclass_or_select: DataClass | Select | None
    ):
        # Add table or select to self.join_tables
        if dataclass_or_select is not None and isinstance(dataclass_or_select, Select):
            # Treat the select just like a table.
            select_statement = dataclass_or_select.get_statement()
            self.join_tables.append(f"( {select_statement} )")
            self.parameters = dataclass_or_select.parameters + self.parameters
        else:
            self.join_tables.append(join_table)

    def _handle_right_join_type(
        self,
        left_type: DataClassType,
        other_class: DataClassType,
        other_attribute: str | None,
    ):
        # Determine the right type
        if other_attribute is None:
            self.join_other_attributes.append(SQLITE_ROWID)
            right_type = other_class
        else:
            self.join_other_attributes.append(other_attribute)
            other_field = find_field(other_attribute, other_class)
            right_type = other_field.type

        if left_type is not right_type:
            raise TypeError(
                f"The type of the left side of the join is {left_type} but the "
                f"type of the right side of the join is {right_type}"
            )

    def __call__(
        self,
        connection: ConnectionContextMgr | None = None,
    ) -> Iterator[DataClass]:
        with self.orm.connection_context(connection) as con:
            cursor = con.cursor()
            cursor.execute(self.get_statement(), tuple(self.parameters))
            data = cursor.fetchall()

        return self.orm._query_results_to_instances(data, self.dataclass, self.fields)


def execute_with_parameters(
    connection: Connection, single_query: str, parameters: Tuple[SQLParameter, ...]
) -> KeyType | None:
    cursor = connection.cursor()
    cursor.execute(single_query, parameters)

    lastrowid = cursor.lastrowid
    return KeyType(lastrowid) if lastrowid is not None else None


def get_instance_fields(
    instance_or_class: DataClass | DataClassType,
) -> tuple[str, tuple[Field, ...]]:
    if not dataclasses.is_dataclass(instance_or_class):
        raise TypeError(
            f"{type(instance_or_class)} hasn't been decorated with @dataclass"
        )

    cls = get_class(instance_or_class)
    fields = [
        Field(name=k, type_hint=v, non_null_type=resolve_type(v))
        for k, v in get_type_hints(cls).items()
    ]
    if len(fields) == 0:
        raise ValueError(f"{instance_or_class} has no fields.")

    # Use the class name as the table name
    table = cls.__name__
    return table, tuple(fields)


def get_class(instance_or_class: DataClass | DataClassType) -> DataClassType:
    if isinstance(instance_or_class, type):
        cls = instance_or_class
    else:
        cls = type(instance_or_class)
    return cls


def resolve_type(cls: DataClassType):
    if type(cls) is UnionType or cls.__name__ == "Optional":
        types = get_args(cls)
        if len(types) > 2 or NoneType not in types:
            raise TypeError(f"{cls}: Union type must be NoneType and one other type")
        return next(filter(lambda t: t != NoneType, types))
    else:
        return cls


def comma_separated_names(
    fields: tuple[Field, ...],
    include_rowid=True,
    table=None,
) -> str:
    if table is None:
        field_names = [_.name for _ in fields]
        rowid = SQLITE_ROWID
    else:
        field_names = [".".join((table, _.name)) for _ in fields]
        rowid = ".".join((table, SQLITE_ROWID))

    names = ", ".join(field_names)
    if include_rowid:
        return ", ".join((rowid, names))
    else:
        return names


def find_field(name: str, dataclass: DataClassType):
    """
    Args:
        name (str): Field name
        dataclass (DataClass): DataClass having `name` as a member

    Raises:
        ValueError: Raised if `name` is not an attribute of dataclass

    Returns:
        Field: The dataclass field.
    """
    field = next(filter(lambda f: f.name == name, dataclasses.fields(dataclass)), None)
    if field is None:
        raise ValueError(f"{name} is not a field of {dataclass}")

    return field


def validate_join_arguments(
    attribute: str | None,
    dataclass_or_select: DataClass | Select | None,
    other_attribute: str | None,
):
    if attribute is None and dataclass_or_select is None:
        raise ValueError('"attribute" and "dataclass_or_select" cannot both be None')

    if attribute is None and other_attribute is None:
        raise ValueError('"attribute" and "otherclass_attribute" cannot both be None')

    if dataclass_or_select is None and other_attribute is not None:
        raise ValueError(
            '"otherclass_attribute" cannot be specified without "dataclass_or_select"'
        )
