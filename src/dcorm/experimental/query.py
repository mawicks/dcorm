import dataclasses
from sandbox.dcorm.connection_pool import ConnectionContextMgr
from sandbox.dcorm.dcorm import ORM, SQLITE_ROWID, comma_separated_names
from sandbox.dcorm.types import DataClass, DataClassType, Field, SQLParameter


from typing import Any, Iterator


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
