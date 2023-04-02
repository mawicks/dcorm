from dataclasses import dataclass
import pytest
import sqlite3
from typing import cast

from sandbox.dcorm.queries import Select
from sandbox.dcorm.dcorm import (
    orm_dataclass,
    create,
    insert,
    set_connection_factory,
)


@orm_dataclass
@dataclass
class Student:
    name: str


@orm_dataclass
@dataclass
class Instructor:
    name: str


@orm_dataclass
@dataclass
class Course:
    title: str
    instructor: Instructor


@orm_dataclass
@dataclass
class Registration:
    student: Student
    course: Course


class RegistrationDatabase:
    def __init__(self):
        pass

    def initialize_tables(self):
        create(Student, drop_if_exists=True)
        create(Instructor, drop_if_exists=True)
        create(Course, drop_if_exists=True)
        create(Registration, drop_if_exists=True)

    def create_student(self, name) -> Student:
        student = Student(name)
        insert(student)
        return student

    def create_instructor(self, name) -> Instructor:
        instructor = Instructor(name)
        insert(instructor)
        return instructor

    def create_course(self, title, instructor) -> Course:
        course = Course(title, instructor)
        insert(course)
        return course

    def register(self, student: Student, course: Course):
        insert(Registration(student, course))

    def init(self):
        self.initialize_tables()

        alice = self.create_student("Alice")
        bob = self.create_student("Bob")
        charlie = self.create_student("Charlie")

        jones = self.create_instructor("Jones")
        kaplan = self.create_instructor("Kaplan")

        algebra = self.create_course("Algebra", jones)
        biology = self.create_course("Biology", kaplan)
        calculus = self.create_course("Calculus", kaplan)

        self.register(alice, algebra)
        self.register(alice, biology)
        self.register(alice, calculus)

        self.register(bob, algebra)
        self.register(bob, biology)

        self.register(charlie, calculus)


@pytest.fixture
def empty_db():
    connection = sqlite3.connect(":memory:")
    set_connection_factory(lambda: connection)
    return connection


@pytest.fixture
def registration_database(empty_db):
    set_connection_factory(lambda: empty_db)
    registrations = RegistrationDatabase()
    registrations.init()
    return empty_db


# Tests for queries involving a relation
def test_registrations_where_equal_algebra_returns_two(registration_database):
    set_connection_factory(lambda: registration_database)
    algebra = next(iter(Select(Course).where("title = ?", ("Algebra",))()))
    registrations = Select(Registration).where_equal("course", algebra)()
    assert len(list(registrations)) == 2


# Tests for queries involving a relation
def test_registrations_where_with_object_substitution_for_algebra_returns_two(
    registration_database,
):
    set_connection_factory(lambda: registration_database)
    algebra = next(iter(Select(Course).where("title = ?", ("Algebra",))()))
    registrations = Select(Registration).where("course = ?", (algebra,))()
    assert len(list(registrations)) == 2


@pytest.mark.parametrize(
    "course,expected_students",
    [
        ("Algebra", {"Alice", "Bob"}),
        ("Biology", {"Alice", "Bob"}),
        ("Calculus", {"Alice", "Charlie"}),
    ],
)
def test_class_list_query(registration_database, course, expected_students):
    set_connection_factory(lambda: registration_database)
    query = Select(Registration).join("course").where("course.title = ?", (course,))
    class_list = [
        cast(Registration, registration).student.name for registration in query()
    ]
    # Check the length before turning into a set to ensure there are no duplicates.
    assert set(class_list) == expected_students


@pytest.mark.parametrize(
    "student,expected_courses",
    [
        ("Alice", {"Algebra", "Biology", "Calculus"}),
        ("Bob", {"Algebra", "Biology"}),
        ("Charlie", {"Calculus"}),
    ],
)
def test_student_schedule_query(registration_database, student, expected_courses):
    set_connection_factory(lambda: registration_database)
    query = Select(Registration).join("student").where("student.name = ?", (student,))
    class_list = [
        cast(Registration, registration).course.title for registration in query()
    ]
    # Check the length before turning into a set to ensure there are no duplicates.
    assert set(class_list) == expected_courses


@pytest.mark.parametrize(
    "instructor,expected_courses",
    [
        ("Kaplan", {"Biology", "Calculus"}),
        ("Jones", {"Algebra"}),
    ],
)
def test_instructor_schedule_query(registration_database, instructor, expected_courses):
    set_connection_factory(lambda: registration_database)
    query = (
        Select(Course).join("instructor").where("instructor.name = ?", (instructor,))
    )
    class_list = [cast(Course, course).title for course in query()]
    # Check the length before turning into a set to ensure there are no duplicates.
    assert set(class_list) == expected_courses


@pytest.mark.parametrize(
    "instructor,expected_students",
    [
        ("Jones", {"Alice", "Bob"}),
        ("Kaplan", {"Alice", "Bob", "Charlie"}),
    ],
)
def test_students_having_instructor(
    registration_database, instructor, expected_students
):
    set_connection_factory(lambda: registration_database)
    instructor_courses = (
        Select(Course).join("instructor").where("instructor.name = ?", (instructor,))
    )
    instructor_registrations = Select(Registration).join("course", instructor_courses)
    query = Select(Student).join(None, instructor_registrations, "student")
    students = [cast(Student, student).name for student in query()]
    assert set(students) == expected_students
